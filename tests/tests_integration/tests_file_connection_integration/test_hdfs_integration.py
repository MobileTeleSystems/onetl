from __future__ import annotations

import logging
import shutil
from getpass import getuser
from pathlib import Path

import pytest

from onetl.hooks import hook

pytestmark = pytest.mark.hdfs


def test_hdfs_check(hdfs_connection, caplog):
    with caplog.at_level(logging.INFO):
        assert hdfs_connection.check() == hdfs_connection

    assert "type = HDFS" in caplog.text
    assert f"host = '{hdfs_connection.host}'" in caplog.text
    assert f"port = {hdfs_connection.webhdfs_port}" in caplog.text
    assert "user = " not in caplog.text
    assert "timeout = 10" in caplog.text
    assert "keytab =" not in caplog.text

    if hdfs_connection.password:
        assert hdfs_connection.password.get_secret_value() not in caplog.text

    assert "Connection is available" in caplog.text


def test_hdfs_check_with_keytab(mocker, hdfs_server, caplog, request, tmp_path_factory):
    from onetl.connection import HDFS
    from onetl.connection.file_connection import hdfs

    mocker.patch.object(hdfs, "kinit")

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
    assert f"port = {hdfs.webhdfs_port}" in caplog.text
    assert f"user = '{hdfs.user}'" in caplog.text
    assert f"keytab = '{keytab}' (kind='file'" in caplog.text
    assert "timeout = 10" in caplog.text
    assert "password =" not in caplog.text

    assert "Connection is available" in caplog.text


def test_hdfs_check_with_password(mocker, hdfs_server, caplog):
    from onetl.connection import HDFS
    from onetl.connection.file_connection import hdfs

    mocker.patch.object(hdfs, "kinit")

    hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port, user=getuser(), password="somepass")

    with caplog.at_level(logging.INFO):
        assert hdfs.check()

    assert "type = HDFS" in caplog.text
    assert f"host = '{hdfs.host}'" in caplog.text
    assert f"port = {hdfs.webhdfs_port}" in caplog.text
    assert f"user = '{hdfs.user}'" in caplog.text
    assert "timeout = 10" in caplog.text
    assert "keytab =" not in caplog.text

    assert "somepass" not in caplog.text

    assert "Connection is available" in caplog.text


def test_hdfs_wrong_source_check_error():
    from onetl.connection import HDFS

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        HDFS(host="hive1", port=1234).check()


def test_hdfs_check_with_hooks(request, hdfs_server):
    from onetl.connection import HDFS

    @HDFS.slots.is_namenode_active.bind
    @hook
    def is_namenode_active(host: str, cluster: str | None) -> bool:
        return host == hdfs_server.host

    request.addfinalizer(is_namenode_active.disable)

    HDFS(host=hdfs_server.host, port=hdfs_server.port).check()  # no exception

    with pytest.raises(RuntimeError, match="Host 'some-node2.domain.com' is not an active namenode"):
        HDFS(host="some-node2.domain.com").check()

    with pytest.raises(
        RuntimeError,
        match="Host 'some-node2.domain.com' is not an active namenode of cluster 'rnd-dwh'",
    ):
        HDFS(host="some-node2.domain.com", cluster="rnd-dwh").check()

    with pytest.raises(RuntimeError, match="Cannot get list of namenodes for a cluster 'rnd-dwh'"):
        HDFS(cluster="rnd-dwh").check()

    @HDFS.slots.get_cluster_namenodes.bind
    @hook
    def get_cluster_namenodes(cluster: str) -> set[str]:
        if cluster == "rnd-dwh":
            return {hdfs_server.host}
        return {"some-node1.domain.com"}

    request.addfinalizer(get_cluster_namenodes.disable)

    @HDFS.slots.get_webhdfs_port.bind
    @hook
    def get_webhdfs_port(cluster: str) -> int | None:
        if cluster == "rnd-dwh":
            return hdfs_server.port
        return None

    request.addfinalizer(get_webhdfs_port.disable)

    # no exception
    assert HDFS(cluster="rnd-dwh").check()

    with pytest.raises(RuntimeError, match="Cannot detect active namenode for cluster 'rnd-prod'"):
        HDFS(cluster="rnd-prod").check()

    @HDFS.slots.get_current_cluster.bind
    @hook
    def get_current_cluster() -> str:
        return "rnd-dwh"

    request.addfinalizer(get_current_cluster.disable)
    assert HDFS.get_current().check()
