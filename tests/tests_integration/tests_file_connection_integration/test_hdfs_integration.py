import logging
import shutil
from getpass import getuser
from pathlib import Path
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
    assert "timeout = 10" in caplog.text
    assert "keytab =" not in caplog.text

    if hdfs_connection.password:
        assert hdfs_connection.password.get_secret_value() not in caplog.text

    assert "Connection is available" in caplog.text


@patch.object(hdfs, "kinit")
def test_hdfs_check_with_keytab(kinit, hdfs_server, caplog, request, tmp_path_factory):
    folder: Path = tmp_path_factory.mktemp("keytab")
    folder.mkdir(exist_ok=True, parents=True)
    keytab = folder / "user.keytab"
    keytab.touch()
    hdfs = HDFS(host="some_host", user="some_user", keytab=keytab)

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port, user=getuser(), keytab=keytab)

    with caplog.at_level(logging.INFO):
        assert hdfs.check()

    assert "type = HDFS" in caplog.text
    assert f"host = '{hdfs.host}'" in caplog.text
    assert f"port = {hdfs.port}" in caplog.text
    assert f"user = '{hdfs.user}'" in caplog.text
    assert f"keytab = '{keytab}' (kind='file'" in caplog.text
    assert "timeout = 10" in caplog.text
    assert "password =" not in caplog.text

    assert "Connection is available" in caplog.text


@patch.object(hdfs, "kinit")
def test_hdfs_check_with_password(kinit, hdfs_server, caplog):
    hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port, user=getuser(), password="somepass")

    with caplog.at_level(logging.INFO):
        assert hdfs.check()

    assert "type = HDFS" in caplog.text
    assert f"host = '{hdfs.host}'" in caplog.text
    assert f"port = {hdfs.port}" in caplog.text
    assert f"user = '{hdfs.user}'" in caplog.text
    assert "timeout = 10" in caplog.text
    assert "keytab =" not in caplog.text

    assert "somepass" not in caplog.text

    assert "Connection is available" in caplog.text


def test_hdfs_wrong_source_check():
    hdfs = HDFS(host="hive1", port=1234)

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        hdfs.check()
