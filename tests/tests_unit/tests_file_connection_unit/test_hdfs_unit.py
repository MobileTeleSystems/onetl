from __future__ import annotations

import re
import shutil
from pathlib import Path

import pytest

from onetl.connection import FileConnection
from onetl.hooks import hook

pytestmark = [pytest.mark.hdfs, pytest.mark.file_connection, pytest.mark.connection]


def test_hdfs_connection_with_host():
    from onetl.connection import HDFS

    conn = HDFS(host="some-host.domain.com")
    assert isinstance(conn, FileConnection)
    assert conn.host == "some-host.domain.com"
    assert conn.webhdfs_port == 50070
    assert not conn.user
    assert not conn.password
    assert not conn.keytab
    assert conn.instance_url == "hdfs://some-host.domain.com:50070"
    assert str(conn) == "HDFS[some-host.domain.com:50070]"


def test_hdfs_connection_with_cluster():
    from onetl.connection import HDFS

    conn = HDFS(cluster="rnd-dwh")
    assert conn.cluster == "rnd-dwh"
    assert conn.webhdfs_port == 50070
    assert not conn.user
    assert not conn.password
    assert not conn.keytab
    assert conn.instance_url == "rnd-dwh"
    assert str(conn) == "HDFS[rnd-dwh]"


def test_hdfs_connection_with_cluster_and_host():
    from onetl.connection import HDFS

    conn = HDFS(cluster="rnd-dwh", host="some-host.domain.com")
    assert conn.cluster == "rnd-dwh"
    assert conn.host == "some-host.domain.com"
    assert conn.instance_url == "rnd-dwh"
    assert str(conn) == "HDFS[rnd-dwh]"


def test_hdfs_connection_with_host_and_port():
    from onetl.connection import HDFS

    conn1 = HDFS(host="some-host.domain.com", webhdfs_port=9080)
    assert conn1.host == "some-host.domain.com"
    assert conn1.webhdfs_port == 9080
    assert conn1.instance_url == "hdfs://some-host.domain.com:9080"
    assert str(conn1) == "HDFS[some-host.domain.com:9080]"

    conn2 = HDFS(host="some-host.domain.com", port=9080)
    assert conn1.webhdfs_port == conn2.webhdfs_port
    assert conn1 == conn2


def test_hdfs_connection_with_user():
    from onetl.connection import HDFS

    conn = HDFS(host="some-host.domain.com", user="some_user")
    assert conn.host == "some-host.domain.com"
    assert conn.webhdfs_port == 50070
    assert conn.user == "some_user"
    assert not conn.password
    assert not conn.keytab


def test_hdfs_connection_with_password():
    from onetl.connection import HDFS

    conn = HDFS(host="some-host.domain.com", user="some_user", password="pwd")
    assert conn.host == "some-host.domain.com"
    assert conn.webhdfs_port == 50070
    assert conn.user == "some_user"
    assert conn.password != "pwd"
    assert conn.password.get_secret_value() == "pwd"
    assert not conn.keytab
    assert str(conn) == "HDFS[some-host.domain.com:50070]"

    assert "pwd" not in repr(conn)


def test_hdfs_connection_with_keytab(request, tmp_path_factory):
    from onetl.connection import HDFS

    folder: Path = tmp_path_factory.mktemp("keytab")
    folder.mkdir(exist_ok=True, parents=True)
    keytab = folder / "user.keytab"
    keytab.touch()
    conn = HDFS(host="some-host.domain.com", user="some_user", keytab=keytab)

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    assert conn.user == "some_user"
    assert not conn.password


def test_hdfs_connection_keytab_does_not_exist():
    from onetl.connection import HDFS

    with pytest.raises(ValueError, match='file or directory at path "/path/to/keytab" does not exist'):
        HDFS(host="some-host.domain.com", user="some_user", keytab="/path/to/keytab")


def test_hdfs_connection_keytab_is_directory(request, tmp_path_factory):
    from onetl.connection import HDFS

    folder: Path = tmp_path_factory.mktemp("keytab")
    keytab = folder / "user.keytab"
    keytab.mkdir(exist_ok=True, parents=True)

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    with pytest.raises(ValueError, match=f'path "{keytab}" does not point to a file'):
        HDFS(host="some-host.domain.com", user="some_user", keytab=keytab)


def test_hdfs_connection_without_cluster_and_host():
    from onetl.connection import HDFS

    with pytest.raises(ValueError):
        HDFS()


def test_hdfs_connection_with_password_and_keytab(request, tmp_path_factory):
    from onetl.connection import HDFS

    folder: Path = tmp_path_factory.mktemp("keytab")
    folder.mkdir(exist_ok=True, parents=True)
    keytab = folder / "user.keytab"
    keytab.touch()

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    with pytest.raises(ValueError, match="Please provide either `keytab` or `password` for kinit, not both"):
        HDFS(host="hdfs2", webhdfs_port=50070, user="usr", password="pwd", keytab=keytab)  # noqa: F841


def test_hdfs_get_known_clusters_hook(request):
    from onetl.connection import HDFS

    @HDFS.Slots.get_known_clusters.bind
    @hook
    def get_known_clusters() -> set[str]:
        return {"known1", "known2"}

    request.addfinalizer(get_known_clusters.disable)

    with pytest.raises(
        ValueError,
        match=r"Cluster 'unknown' is not in the known clusters list: \['known1', 'known2'\]",
    ):
        HDFS(cluster="unknown")

    HDFS(cluster="known1")  # no exception


def test_hdfs_known_normalize_cluster_name_hook(request):
    from onetl.connection import HDFS

    @HDFS.Slots.normalize_cluster_name.bind
    @hook
    def normalize_cluster_name(cluster: str) -> str:
        return cluster.lower().replace("_", "-")

    request.addfinalizer(normalize_cluster_name.disable)

    assert HDFS(cluster="rnd-dwh").cluster == "rnd-dwh"
    assert HDFS(cluster="rnd_dwh").cluster == "rnd-dwh"
    assert HDFS(cluster="RND-DWH").cluster == "rnd-dwh"


def test_hdfs_get_cluster_namenodes_hook(request):
    from onetl.connection import HDFS

    @HDFS.Slots.get_cluster_namenodes.bind
    @hook
    def get_cluster_namenodes(cluster: str) -> set[str]:
        return {"some-node1.domain.com", "some-node2.domain.com"}

    request.addfinalizer(get_cluster_namenodes.disable)

    error_msg = (
        "Namenode 'unknown.domain.com' is not in the known nodes list of cluster 'rnd-dwh': "
        r"\['some-node1.domain.com', 'some-node2.domain.com'\]"
    )
    with pytest.raises(ValueError, match=error_msg):
        HDFS(cluster="rnd-dwh", host="unknown.domain.com")

    HDFS(cluster="rnd-dwh", host="some-node1.domain.com")  # no exception


def test_hdfs_normalize_namenode_host_hook(request):
    from onetl.connection import HDFS

    @HDFS.Slots.normalize_namenode_host.bind
    @hook
    def normalize_namenode_host(host: str, cluster: str | None) -> str:
        host = host.lower()
        if cluster == "rnd-dwh":
            if not host.endswith(".domain.com"):
                host += ".domain.com"
        return host

    request.addfinalizer(normalize_namenode_host.disable)

    assert HDFS(host="some-node.domain.com").host == "some-node.domain.com"
    assert HDFS(host="SOME-NODE.DOMAIN.COM").host == "some-node.domain.com"
    assert HDFS(host="some-node", cluster="rnd-dwh").host == "some-node.domain.com"
    assert HDFS(host="some-node", cluster="rnd-prod").host == "some-node"


def test_hdfs_get_webhdfs_port_hook(request):
    from onetl.connection import HDFS

    @HDFS.Slots.get_webhdfs_port.bind
    @hook
    def get_webhdfs_port(cluster: str) -> int | None:
        if cluster == "rnd-dwh":
            return 9080
        return None

    request.addfinalizer(get_webhdfs_port.disable)

    assert HDFS(cluster="rnd-dwh").webhdfs_port == 9080
    assert HDFS(cluster="rnd-prod").webhdfs_port == 50070

    assert HDFS(host="some-node.domain.com").webhdfs_port == 50070
    assert HDFS(host="some-node.domain.com", cluster="rnd-dwh").webhdfs_port == 9080


def test_hdfs_known_get_current(request):
    from onetl.connection import HDFS

    # no hooks bound to HDFS.Slots.get_current_cluster
    error_msg = re.escape(
        "HDFS.get_current() can be used only if there are some hooks bound to HDFS.Slots.get_current_cluster",
    )
    with pytest.raises(RuntimeError, match=error_msg):
        HDFS.get_current()

    @HDFS.Slots.get_current_cluster.bind
    @hook
    def get_current_cluster() -> str:
        return "rnd-dwh"

    request.addfinalizer(get_current_cluster.disable)

    conn = HDFS.get_current()
    assert conn.cluster == "rnd-dwh"
