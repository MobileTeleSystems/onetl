import pytest

from onetl.connection import HDFS, FileConnection


def test_hdfs_connection():
    hdfs = HDFS(host="some_host", user="some_user", password="pwd")
    assert isinstance(hdfs, FileConnection)
    assert hdfs.port == 50070


def test_hdfs_connection_with_password_and_keytab():
    with pytest.raises(ValueError):
        HDFS(host="hive2", port=50070, user="usr", password="pwd", keytab="/path/to/keytab")  # noqa: F841
