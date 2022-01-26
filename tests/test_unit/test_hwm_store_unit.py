import pytest
import secrets
import tempfile

from omegaconf import OmegaConf

from onetl.strategy.hwm_store import AtlasHWMStore, HWMStoreManager, MemoryHWMStore, YAMLHWMStore, detect_hwm_store


@pytest.mark.parametrize(
    "hwm_store",
    [
        MemoryHWMStore(),
        YAMLHWMStore(path=tempfile.mktemp("hwmstore")),  # noqa: S306 NOSONAR
        AtlasHWMStore(
            url="http://some.atlas.url",
            user=secrets.token_hex(),
            password=secrets.token_hex(),
        ),
    ],
)
def test_hwm_store_unit_context_manager(hwm_store):
    with hwm_store as store:
        assert HWMStoreManager.get_current() == store

    assert HWMStoreManager.get_current() != hwm_store
    assert isinstance(HWMStoreManager.get_current(), YAMLHWMStore)


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
            MemoryHWMStore,
            {"hwm_store": {"memory": None}},
        ),
        (
            MemoryHWMStore,
            {"hwm_store": {"memory": []}},
        ),
        (
            MemoryHWMStore,
            {"hwm_store": {"memory": {}}},
        ),
        (
            YAMLHWMStore,
            {"hwm_store": {"yml": tempfile.mktemp("hwmstore")}},  # noqa: S306 NOSONAR
        ),
        (
            YAMLHWMStore,
            {"hwm_store": {"yml": [tempfile.mktemp("hwmstore")]}},  # noqa: S306 NOSONAR
        ),
        (
            YAMLHWMStore,
            {"hwm_store": {"yml": {"path": tempfile.mktemp("hwmstore"), "encoding": "utf8"}}},  # noqa: S306 NOSONAR
        ),
        (
            YAMLHWMStore,
            {"hwm_store": {"yml": [tempfile.mktemp("hwmstore"), "utf8"]}},  # noqa: S306 NOSONAR
        ),
        (
            AtlasHWMStore,
            {"hwm_store": {"atlas": "http://some.atlas.url"}},
        ),
        (
            AtlasHWMStore,
            {"hwm_store": {"atlas": {"url": "http://some.atlas.url"}}},
        ),
        (
            AtlasHWMStore,
            {"hwm_store": {"atlas": ["http://some.atlas.url"]}},
        ),
        (
            AtlasHWMStore,
            {
                "hwm_store": {
                    "atlas": {
                        "url": "http://some.atlas.url",
                        "user": secrets.token_hex(),
                        "password": secrets.token_hex(),
                    },
                },
            },
        ),
        (
            AtlasHWMStore,
            {
                "hwm_store": {
                    "atlas": [
                        "http://some.atlas.url",
                        secrets.token_hex(),
                        secrets.token_hex(),
                    ],
                },
            },
        ),
    ],
)
@pytest.mark.parametrize("config_constructor", [dict, OmegaConf.create])
def test_hwm_store_unit_detect(hwm_store_class, input_config, config_constructor):
    @detect_hwm_store
    def main(config):
        assert isinstance(HWMStoreManager.get_current(), hwm_store_class)

    conf = config_constructor(input_config)
    main(conf)

    conf = config_constructor({"nested": input_config})
    main(conf)

    conf = config_constructor({"even": {"more": {"nested": input_config}}})
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
    @detect_hwm_store
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
        {"hwm_store": {"yml": {"unknown": "arg"}}},
        {"hwm_store": {"yml": ["too", "many", "args"]}},
        {"hwm_store": {"atlas": 1}},
        {"hwm_store": {"atlas": None}},
        {"hwm_store": {"atlas": []}},
        {"hwm_store": {"atlas": {}}},
        {"hwm_store": {"atlas": {"unknown": "arg"}}},
        {"hwm_store": {"atlas": ["too", "many", "args", "abc"]}},
    ],
)
@pytest.mark.parametrize("config_constructor", [dict, OmegaConf.create])
def test_hwm_store_unit_wrong_options(input_config, config_constructor):
    @detect_hwm_store
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
