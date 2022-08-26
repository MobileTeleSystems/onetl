import shutil
from pathlib import Path

import pytest

from onetl.connection import HDFS, FileConnection


def test_hdfs_connection():
    hdfs = HDFS(host="some_host")
    assert isinstance(hdfs, FileConnection)
    assert hdfs.host == "some_host"
    assert hdfs.port == 50070
    assert not hdfs.user
    assert not hdfs.password
    assert not hdfs.keytab


def test_hdfs_connection_with_port():
    hdfs = HDFS(host="some_host", port=500)
    assert isinstance(hdfs, FileConnection)
    assert hdfs.host == "some_host"
    assert hdfs.port == 500
    assert not hdfs.user
    assert not hdfs.password
    assert not hdfs.keytab


def test_hdfs_connection_with_user():
    hdfs = HDFS(host="some_host", user="some_user")
    assert hdfs.host == "some_host"
    assert hdfs.port == 50070
    assert hdfs.user == "some_user"
    assert not hdfs.password
    assert not hdfs.keytab


def test_hdfs_connection_with_password():
    hdfs = HDFS(host="some_host", user="some_user", password="pwd")
    assert hdfs.host == "some_host"
    assert hdfs.port == 50070
    assert hdfs.user == "some_user"
    assert hdfs.password != "pwd"
    assert hdfs.password.get_secret_value() == "pwd"
    assert not hdfs.keytab


def test_hdfs_connection_with_keytab(request, tmp_path_factory):
    folder: Path = tmp_path_factory.mktemp("keytab")
    folder.mkdir(exist_ok=True, parents=True)
    keytab = folder / "user.keytab"
    keytab.touch()
    hdfs = HDFS(host="some_host", user="some_user", keytab=keytab)

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    assert hdfs.host == "some_host"
    assert hdfs.port == 50070
    assert hdfs.user == "some_user"
    assert not hdfs.password


def test_hdfs_connection_keytab_does_not_exist():
    with pytest.raises(ValueError, match='file or directory at path "/path/to/keytab" does not exist'):
        HDFS(host="some_host", user="some_user", keytab="/path/to/keytab")


def test_hdfs_connection_keytab_is_directory(request, tmp_path_factory):
    folder: Path = tmp_path_factory.mktemp("keytab")
    keytab = folder / "user.keytab"
    keytab.mkdir(exist_ok=True, parents=True)

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    with pytest.raises(ValueError, match=f'path "{keytab}" does not point to a file'):
        HDFS(host="some_host", user="some_user", keytab=keytab)


def test_hdfs_connection_without_mandatory_args():
    with pytest.raises(ValueError):
        HDFS()


def test_hdfs_connection_with_password_and_keytab(request, tmp_path_factory):
    folder: Path = tmp_path_factory.mktemp("keytab")
    folder.mkdir(exist_ok=True, parents=True)
    keytab = folder / "user.keytab"
    keytab.touch()

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    with pytest.raises(ValueError, match="Please provide either `keytab` or `password` for kinit, not both"):
        HDFS(host="hive2", port=50070, user="usr", password="pwd", keytab=keytab)  # noqa: F841
