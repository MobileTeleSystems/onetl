import secrets
import shutil
from pathlib import Path

import pytest

from onetl.hwm.store import YAMLHWMStore


def test_hwm_store_integration_yaml_path(request, tmp_path_factory, hwm_delta):
    hwm, _delta = hwm_delta
    folder: Path = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex(5)

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    store = YAMLHWMStore(path=path)

    assert path.exists()

    assert not list(path.glob("**/*"))

    store.save(hwm)

    empty = True
    for item in path.glob("**/*"):
        empty = False
        assert item.is_file()
        assert item.suffix == ".yml"

    assert not empty


def test_hwm_store_integration_yaml_path_not_folder(request, tmp_path_factory):
    folder: Path = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex(5)
    path.touch()

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    with pytest.raises(OSError):
        YAMLHWMStore(path=path)


def test_hwm_store_integration_yaml_path_no_access(request, tmp_path_factory, hwm_delta):
    hwm, _delta = hwm_delta
    folder: Path = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex(5)
    path.mkdir()
    path.chmod(000)

    def finalizer():
        path.chmod(0o777)
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    store = YAMLHWMStore(path=path)

    with pytest.raises(OSError):
        store.get(hwm.qualified_name)

    with pytest.raises(OSError):
        store.save(hwm)
