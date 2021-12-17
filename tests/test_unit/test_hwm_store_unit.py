# noinspection PyPackageRequirements

import secrets
import tempfile
import pytest

from omegaconf import OmegaConf

from etl_entities import Column, Table, IntHWM
from onetl.strategy.hwm_store import YAMLHWMStore, MemoryHWMStore, HWMStoreManager, detect_hwm_store


@pytest.mark.parametrize(
    "hwm_store_class",
    [
        YAMLHWMStore,
        MemoryHWMStore,
    ],
)
def test_postgres_hwm_store_unit(hwm_store_class):
    store = hwm_store_class()

    table = Table(name="abc", db="cde", instance="proto://domain.com")
    column = Column(name="def")
    hwm = IntHWM(column=column, table=table)
    assert store.get(hwm.qualified_name) is None

    store.save(hwm)
    assert store.get(hwm.qualified_name) == hwm


def test_postgres_hwm_store_unit_yaml_path(tmp_path_factory):
    folder = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex()

    store = YAMLHWMStore(path)

    assert store.path == path
    assert path.exists()

    assert not list(path.glob("**/*"))

    table = Table(name="abc", db="cde", instance="proto://domain.com")
    column = Column(name="def")
    hwm = IntHWM(column=column, table=table)
    store.save(hwm)

    empty = True
    for item in path.glob("**/*"):
        empty = False
        assert item.is_file()
        assert item.suffix == ".yml"

    assert not empty


def test_postgres_hwm_store_unit_yaml_path_not_folder(tmp_path_factory):
    folder = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex()
    path.touch()

    with pytest.raises(OSError):
        YAMLHWMStore(path)


def test_postgres_hwm_store_unit_yaml_path_no_access(tmp_path_factory):
    folder = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex()
    path.mkdir()
    path.chmod(000)

    store = YAMLHWMStore(path)
    table = Table(name="abc", db="cde", instance="proto://domain.com")
    column = Column(name="def")
    hwm = IntHWM(column=column, table=table)

    with pytest.raises(OSError):
        store.save(hwm)


@pytest.mark.parametrize(
    "hwm_store_class, input_config",
    [
        (
            YAMLHWMStore,
            {},
        ),
        (
            YAMLHWMStore,
            {"hwm_store": None},
        ),
        (
            YAMLHWMStore,
            {"hwm_store": "yml"},
        ),
        (
            YAMLHWMStore,
            {"hwm_store": "yaml"},
        ),
        (
            MemoryHWMStore,
            {"hwm_store": "memory"},
        ),
        (
            MemoryHWMStore,
            {"hwm_store": "in-memory"},
        ),
        (
            YAMLHWMStore,
            {"hwm_store": {"yml": tempfile.mktemp("hwmstore")}},  # noqa: S306 NOSONAR
        ),
        (
            YAMLHWMStore,
            {"hwm_store": {"yml": {"path": tempfile.mktemp("hwmstore"), "encoding": "utf8"}}},  # noqa: S306 NOSONAR
        ),
        (
            MemoryHWMStore,
            {"hwm_store": {"memory": None}},
        ),
        (
            YAMLHWMStore,
            {"nested": {"hwm_store": "yaml"}},
        ),
        (
            YAMLHWMStore,
            {"even": {"more": {"nested": {"hwm_store": "yml"}}}},
        ),
    ],
)
@pytest.mark.parametrize("config_constructor", [dict, OmegaConf.create])
def test_postgres_hwm_store_unit_detect(hwm_store_class, input_config, config_constructor):
    @detect_hwm_store
    def main(config):
        assert isinstance(HWMStoreManager.get_current(), hwm_store_class)

    conf = config_constructor(input_config)
    main(conf)


@pytest.mark.parametrize(
    "input_config",
    [
        {"hwm_store": 1},
        {"hwm_store": "unknown"},
        {"hwm_store": {"unknown": None}},
    ],
)
@pytest.mark.parametrize("config_constructor", [dict, OmegaConf.create])
def test_postgres_hwm_store_unit_detect_failure(input_config, config_constructor):
    @detect_hwm_store
    def main(config):  # NOSONAR
        pass

    conf = config_constructor(input_config)
    with pytest.raises((KeyError, ValueError)):
        main(conf)
