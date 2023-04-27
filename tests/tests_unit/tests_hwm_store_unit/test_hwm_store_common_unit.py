import tempfile

import pytest
from omegaconf import OmegaConf

from onetl.hwm.store import (
    HWMStoreManager,
    MemoryHWMStore,
    YAMLHWMStore,
    detect_hwm_store,
)

hwm_store = [
    MemoryHWMStore(),
    YAMLHWMStore(path=tempfile.mktemp("hwmstore")),  # noqa: S306 NOSONAR
]


@pytest.mark.parametrize("hwm_store", hwm_store)
def test_hwm_store_get_save(hwm_store, hwm_delta):
    hwm, delta = hwm_delta
    assert hwm_store.get(hwm.qualified_name) is None

    hwm_store.save(hwm)
    assert hwm_store.get(hwm.qualified_name) == hwm

    hwm2 = hwm + delta
    hwm_store.save(hwm2)
    assert hwm_store.get(hwm.qualified_name) == hwm2


@pytest.mark.parametrize(
    "hwm_store_class, input_config, key",
    [
        (
            YAMLHWMStore,
            {"hwm_store": None},
            "hwm_store",
        ),
        (
            YAMLHWMStore,
            {"env": {"hwm_store": "yml"}},
            "env.hwm_store",
        ),
        (
            YAMLHWMStore,
            {"hwm_store": "yml"},
            "hwm_store",
        ),
        (
            YAMLHWMStore,
            {"some_store": "yml"},
            "some_store",
        ),
        (
            YAMLHWMStore,
            {"hwm_store": "yaml"},
            "hwm_store",
        ),
        (
            MemoryHWMStore,
            {"hwm_store": "memory"},
            "hwm_store",
        ),
        (
            MemoryHWMStore,
            {"some_store": "memory"},
            "some_store",
        ),
        (
            MemoryHWMStore,
            {"hwm_store": "in-memory"},
            "hwm_store",
        ),
        (
            MemoryHWMStore,
            {"hwm_store": {"memory": None}},
            "hwm_store",
        ),
        (
            MemoryHWMStore,
            {"hwm_store": {"memory": []}},
            "hwm_store",
        ),
        (
            MemoryHWMStore,
            {"hwm_store": {"memory": {}}},
            "hwm_store",
        ),
        (
            YAMLHWMStore,
            {"hwm_store": {"yml": {"path": tempfile.mktemp("hwmstore"), "encoding": "utf8"}}},  # noqa: S306 NOSONAR
            "hwm_store",
        ),
    ],
)
@pytest.mark.parametrize("config_constructor", [dict, OmegaConf.create])
def test_hwm_store_unit_detect(hwm_store_class, input_config, config_constructor, key):
    @detect_hwm_store(key)
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
def test_hwm_store_unit_detect_failure(input_config, config_constructor):
    @detect_hwm_store("hwm_store")
    def main(config):  # NOSONAR
        pass

    conf = config_constructor(input_config)
    with pytest.raises((KeyError, ValueError)):
        main(conf)

    conf = config_constructor({"nested": input_config})
    with pytest.raises((KeyError, ValueError)):
        main(conf)

    conf = config_constructor({"even": {"more": {"nested": input_config}}})
    with pytest.raises((KeyError, ValueError)):
        main(conf)


@pytest.mark.parametrize(
    "input_config",
    [
        {"hwm_store": {"memory": 1}},
        {"hwm_store": {"memory": {"unknown": "arg"}}},
        {"hwm_store": {"memory": ["too_many_arg"]}},
        {"hwm_store": {"yml": 1}},
        {"hwm_store": {"yml": tempfile.mktemp("hwmstore")}},  # noqa: S306 NOSONAR
        {"hwm_store": {"yml": [tempfile.mktemp("hwmstore")]}},  # noqa: S306 NOSONAR
        {"hwm_store": {"yml": [tempfile.mktemp("hwmstore"), "utf8"]}},  # noqa: S306 NOSONAR
        {"hwm_store": {"yml": {"unknown": "arg"}}},
        {"not_hwm_store": "yml"},
    ],
)
@pytest.mark.parametrize("config_constructor", [dict, OmegaConf.create])
def test_hwm_store_unit_wrong_options(input_config, config_constructor):
    @detect_hwm_store("hwm_store")
    def main(config):  # NOSONAR
        pass

    conf = config_constructor(input_config)

    with pytest.raises((TypeError, ValueError)):
        main(conf)

    conf = config_constructor({"nested": input_config})
    with pytest.raises((TypeError, ValueError)):
        main(conf)

    conf = config_constructor({"even": {"more": {"nested": input_config}}})
    with pytest.raises((TypeError, ValueError)):
        main(conf)


@pytest.mark.parametrize(
    "config, key",
    [
        ({"some": "yml"}, "unknown"),
        ({"some": "yml"}, "some.unknown"),
        ({"some": "yml"}, "some.yaml.unknown"),
        ({"var": {"hwm_store": "yml"}}, "var.hwm_store."),
        ({"var": {"hwm_store": "yml"}}, "var..hwm_store"),
        ({"some": "yml"}, 12),
        ({}, "var.hwm_store"),
        ({"var": {"hwm_store": "yml"}}, ""),
        ({}, ""),
    ],
)
@pytest.mark.parametrize("config_constructor", [dict, OmegaConf.create])
def test_hwm_store_wrong_config_and_key_value_error(config_constructor, config, key):
    with pytest.raises(ValueError):

        @detect_hwm_store(key)
        def main(config):
            ...

        conf = config_constructor(config)
        main(conf)
