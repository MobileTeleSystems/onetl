import secrets

import pytest

from onetl.strategy import YAMLHWMStore


def test_hwm_store_integration_yaml_path(tmp_path_factory, hwm_delta):
    hwm, delta = hwm_delta
    folder = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex(5)

    store = YAMLHWMStore(path)

    assert store.path == path
    assert path.exists()

    assert not list(path.glob("**/*"))

    store.save(hwm)

    empty = True
    for item in path.glob("**/*"):
        empty = False
        assert item.is_file()
        assert item.suffix == ".yml"

    assert not empty


def test_hwm_store_integration_yaml_path_not_folder(tmp_path_factory):
    folder = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex(5)
    path.touch()

    with pytest.raises(OSError):
        YAMLHWMStore(path)


def test_hwm_store_integration_yaml_path_no_access(tmp_path_factory, hwm_delta):
    hwm, _delta = hwm_delta
    folder = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex(5)
    path.mkdir()
    path.chmod(000)

    store = YAMLHWMStore(path)

    with pytest.raises(OSError):
        store.get(hwm.qualified_name)

    with pytest.raises(OSError):
        store.save(hwm)
