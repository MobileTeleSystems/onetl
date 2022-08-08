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
    assert hdfs.password == "pwd"
    assert not hdfs.keytab


def test_hdfs_connection_with_keytab():
    hdfs = HDFS(host="some_host", user="some_user", keytab="/path/to/keytab")
    assert hdfs.host == "some_host"
    assert hdfs.port == 50070
    assert hdfs.user == "some_user"
    assert not hdfs.password


def test_hdfs_connection_without_mandatory_args():
    with pytest.raises(TypeError):
        HDFS()


def test_hdfs_connection_with_password_and_keytab():
    with pytest.raises(ValueError, match="Please provide only `keytab` or only `password` for kinit"):
        HDFS(host="hive2", port=50070, user="usr", password="pwd", keytab="/path/to/keytab")  # noqa: F841
