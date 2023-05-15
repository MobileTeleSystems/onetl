import logging
import secrets
import shutil
from pathlib import Path

import pytest

from onetl.hwm.store import HWMStoreManager, YAMLHWMStore


def test_hwm_store_yaml_path(request, tmp_path_factory, hwm_delta):
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


def test_hwm_store_yaml_path_not_folder(request, tmp_path_factory):
    folder: Path = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex(5)
    path.touch()

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    with pytest.raises(OSError):
        YAMLHWMStore(path=path)


def test_hwm_store_yaml_path_no_access(request, tmp_path_factory, hwm_delta):
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


def test_hwm_store_yaml_context_manager(caplog):
    hwm_store = YAMLHWMStore()

    assert hwm_store.path
    assert hwm_store.encoding == "utf-8"

    with caplog.at_level(logging.INFO):
        with hwm_store as store:
            assert HWMStoreManager.get_current() == store

    assert "|onETL| Using YAMLHWMStore as HWM Store" in caplog.text
    assert "path = '" in caplog.text
    assert "encoding = 'utf-8'" in caplog.text

    assert HWMStoreManager.get_current() == hwm_store


def test_hwm_store_yaml_context_manager_with_path(caplog, request, tmp_path_factory):
    folder: Path = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex(5)

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)
    hwm_store = YAMLHWMStore(path=path)

    assert hwm_store.path == path
    assert hwm_store.encoding == "utf-8"

    with caplog.at_level(logging.INFO):
        with hwm_store as store:
            assert HWMStoreManager.get_current() == store

    assert "|onETL| Using YAMLHWMStore as HWM Store" in caplog.text
    assert f"path = '{path}' (kind='directory'" in caplog.text
    assert "encoding = 'utf-8'" in caplog.text

    assert HWMStoreManager.get_current() != hwm_store
    assert isinstance(HWMStoreManager.get_current(), YAMLHWMStore)


def test_hwm_store_yaml_context_manager_with_encoding(caplog, request, tmp_path_factory):
    folder: Path = tmp_path_factory.mktemp("someconf")
    path = folder / secrets.token_hex(5)

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)
    hwm_store = YAMLHWMStore(path=path, encoding="cp-1251")

    assert hwm_store.path == path
    assert hwm_store.encoding == "cp-1251"

    with caplog.at_level(logging.INFO):
        with hwm_store as store:
            assert HWMStoreManager.get_current() == store

    assert "|onETL| Using YAMLHWMStore as HWM Store" in caplog.text
    assert f"path = '{path}' (kind='directory'" in caplog.text
    assert "encoding = 'cp-1251'" in caplog.text

    assert HWMStoreManager.get_current() != hwm_store
    assert isinstance(HWMStoreManager.get_current(), YAMLHWMStore)


@pytest.mark.parametrize(
    "qualified_name, file_name",
    [
        (
            "id|partition=abc/another=cde#mydb.mytable@dbtype://host.name:1234/schema#dag.task.myprocess@myhost",
            "id__partition_abc_another_cde__mydb.mytable__dbtype_host.name_1234_schema__dag.task.myprocess__myhost",
        ),
        (
            "id#mydb.mytable@dbtype://host.name:1234/schema#dag.task.myprocess@myhost",
            "id__mydb.mytable__dbtype_host.name_1234_schema__dag.task.myprocess__myhost",
        ),
        (
            "column__with__underscores#mydb.mytable@dbtype://host.name:1234/schema#"
            "dag__with__underscores.task_with_underscores.myprocess@myhost",
            "column__with__underscores__mydb.mytable__dbtype_host.name_1234_schema"
            "__dag__with__underscores.task_with_underscores.myprocess__myhost",
        ),
        (
            "id#mydb.mytable@cluster#dag.task.myprocess@myhost",
            "id__mydb.mytable__cluster__dag.task.myprocess__myhost",
        ),
        (
            "id#mydb.mytable@dbtype://host.name:1234/schema#myprocess@myhost",
            "id__mydb.mytable__dbtype_host.name_1234_schema__myprocess__myhost",
        ),
        (
            "downloaded_files#/home/user/abc@ftp://my.domain:23#dag.task.myprocess@myhost",
            "downloaded_files__home_user_abc__ftp_my.domain_23__dag.task.myprocess__myhost",
        ),
        (
            "downloaded_files#/home/user/abc@ftp://my.domain:23#myprocess@myhost",
            "downloaded_files__home_user_abc__ftp_my.domain_23__myprocess__myhost",
        ),
    ],
)
def test_hwm_store_yaml_cleanup_file_name(qualified_name, file_name):
    assert YAMLHWMStore.cleanup_file_name(qualified_name) == file_name
