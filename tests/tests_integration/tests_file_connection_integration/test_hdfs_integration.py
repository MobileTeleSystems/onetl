import logging
from getpass import getuser
from unittest.mock import patch

import pytest

from onetl.connection import HDFS
from onetl.connection.file_connection import hdfs


def test_hdfs_check(hdfs_connection, caplog):
    with caplog.at_level(logging.INFO):
        assert hdfs_connection.check() == hdfs_connection

    assert "type = HDFS" in caplog.text
    assert f"host = '{hdfs_connection.host}'" in caplog.text
    assert f"port = {hdfs_connection.port}" in caplog.text
    assert "user = " not in caplog.text
    assert "timeout = " not in caplog.text
    assert "keytab = " not in caplog.text
    assert "password = " not in caplog.text

    assert "Connection is available" in caplog.text


@patch.object(hdfs, "kinit")
def test_hdfs_check_with_keytab(kinit, hdfs_server, caplog):
    hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port, user=getuser(), password="", keytab="/path/to/keytab")

    with caplog.at_level(logging.INFO):
        assert hdfs.check()

    assert "type = HDFS" in caplog.text
    assert f"host = '{hdfs.host}'" in caplog.text
    assert f"port = {hdfs.port}" in caplog.text
    assert f"user = '{hdfs.user}'" in caplog.text
    assert f"keytab = '{hdfs.keytab}'" in caplog.text
    assert "timeout = " not in caplog.text
    assert "password = " not in caplog.text

    assert "Connection is available" in caplog.text


@patch.object(hdfs, "kinit")
def test_hdfs_check_with_password(kinit, hdfs_server, caplog):
    hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port, user=getuser(), password="pwd")

    with caplog.at_level(logging.INFO):
        assert hdfs.check()

    assert "type = HDFS" in caplog.text
    assert f"host = '{hdfs.host}'" in caplog.text
    assert f"port = {hdfs.port}" in caplog.text
    assert f"user = '{hdfs.user}'" in caplog.text
    assert "timeout = " not in caplog.text
    assert "keytab = " not in caplog.text
    assert "password = " not in caplog.text

    assert "Connection is available" in caplog.text


def test_hdfs_wrong_source_check():
    hdfs = HDFS(host="hive1", port=1234)

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        hdfs.check()
