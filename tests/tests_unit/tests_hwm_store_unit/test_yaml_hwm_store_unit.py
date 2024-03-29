import logging
import secrets
import shutil
import textwrap
from pathlib import Path

import pytest
from etl_entities.hwm_store import BaseHWMStore as OriginalBaseHWMStore
from etl_entities.hwm_store import (
    HWMStoreClassRegistry as OriginalHWMStoreClassRegistry,
)
from etl_entities.hwm_store import HWMStoreStackManager
from etl_entities.hwm_store import HWMStoreStackManager as OriginalHWMStoreManager
from etl_entities.hwm_store import MemoryHWMStore as OriginalMemoryHWMStore
from etl_entities.hwm_store import detect_hwm_store as original_detect_hwm_store
from etl_entities.hwm_store import (
    register_hwm_store_class as original_register_hwm_store_class,
)

from onetl.hwm.store import YAMLHWMStore


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

    store.set_hwm(hwm)

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
        store.get_hwm(hwm.name)

    with pytest.raises(OSError):
        store.set_hwm(hwm)


def test_hwm_store_yaml_context_manager(caplog):
    hwm_store = YAMLHWMStore()

    assert hwm_store.path
    assert hwm_store.encoding == "utf-8"

    with caplog.at_level(logging.INFO):
        with hwm_store as store:
            assert HWMStoreStackManager.get_current() == store

    assert "Using YAMLHWMStore as HWM Store" in caplog.text
    assert "path = " in caplog.text
    assert "encoding = 'utf-8'" in caplog.text

    assert HWMStoreStackManager.get_current() == hwm_store


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
            assert HWMStoreStackManager.get_current() == store

    assert "Using YAMLHWMStore as HWM Store" in caplog.text
    assert str(path) in caplog.text
    assert "encoding = 'utf-8'" in caplog.text

    assert HWMStoreStackManager.get_current() != hwm_store
    assert isinstance(HWMStoreStackManager.get_current(), YAMLHWMStore)


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
            assert HWMStoreStackManager.get_current() == store

    assert "Using YAMLHWMStore as HWM Store" in caplog.text
    assert str(path) in caplog.text
    assert "encoding = 'cp-1251'" in caplog.text

    assert HWMStoreStackManager.get_current() != hwm_store
    assert isinstance(HWMStoreStackManager.get_current(), YAMLHWMStore)


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


def test_hwm_store_no_deprecation_warning_yaml_hwm_store():
    with pytest.warns(None) as record:
        from onetl.hwm.store import YAMLHWMStore

        YAMLHWMStore()
        assert not record


@pytest.mark.parametrize(
    "import_name, original_import",
    [
        ("MemoryHWMStore", OriginalMemoryHWMStore),
        ("BaseHWMStore", OriginalBaseHWMStore),
        ("HWMStoreClassRegistry", OriginalHWMStoreClassRegistry),
        ("HWMStoreManager", OriginalHWMStoreManager),
        ("detect_hwm_store", original_detect_hwm_store),
        ("register_hwm_store_class", original_register_hwm_store_class),
    ],
)
def test_hwm_store_deprecation_warning_matching_cases(import_name, original_import):
    msg = textwrap.dedent(
        f"""
        This import is deprecated since v0.10.0:

            from onetl.hwm.store import {import_name}

        Please use instead:

            from etl_entities.hwm_store import {import_name}
        """,
    )

    with pytest.warns(UserWarning) as record:
        from onetl.hwm.store import __getattr__

        assert __getattr__(import_name) is original_import

        assert record
        assert msg in str(record[0].message)
