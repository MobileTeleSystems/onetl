from __future__ import annotations

import re

import pytest

from onetl.base import BaseFileDFConnection
from onetl.connection import SparkHDFS
from onetl.hooks import hook

pytestmark = [pytest.mark.hdfs, pytest.mark.file_df_connection, pytest.mark.connection]


def test_spark_hdfs_with_cluster(spark_mock):
    conn = SparkHDFS(cluster="rnd-dwh", spark=spark_mock)
    assert isinstance(conn, BaseFileDFConnection)
    assert conn.cluster == "rnd-dwh"
    assert conn.host is None
    assert conn.ipc_port == 8020
    assert conn.instance_url == "rnd-dwh"
    assert str(conn) == "HDFS[rnd-dwh]"


def test_spark_hdfs_with_cluster_and_host(spark_mock):
    conn = SparkHDFS(cluster="rnd-dwh", host="some-host.domain.com", spark=spark_mock)
    assert isinstance(conn, BaseFileDFConnection)
    assert conn.cluster == "rnd-dwh"
    assert conn.host == "some-host.domain.com"
    assert conn.instance_url == "rnd-dwh"
    assert str(conn) == "HDFS[rnd-dwh]"


def test_spark_hdfs_with_port(spark_mock):
    conn1 = SparkHDFS(cluster="rnd-dwh", ipc_port=9020, spark=spark_mock)
    assert isinstance(conn1, BaseFileDFConnection)
    assert conn1.cluster == "rnd-dwh"
    assert conn1.ipc_port == 9020
    assert conn1.instance_url == "rnd-dwh"
    assert str(conn1) == "HDFS[rnd-dwh]"

    conn2 = SparkHDFS(cluster="rnd-dwh", port=9020, spark=spark_mock)
    assert conn1.ipc_port == conn2.ipc_port
    assert conn1 == conn2


def test_spark_hdfs_without_cluster(spark_mock):
    with pytest.raises(ValueError):
        SparkHDFS(spark=spark_mock)

    with pytest.raises(ValueError):
        SparkHDFS(host="some", spark=spark_mock)


def test_spark_hdfs_spark_stopped(spark_stopped):
    msg = "Spark session is stopped. Please recreate Spark session."
    with pytest.raises(ValueError, match=msg):
        SparkHDFS(cluster="rnd-dwh", host="some-host.domain.com", spark=spark_stopped)


def test_spark_hdfs_get_known_clusters_hook(request, spark_mock):
    @SparkHDFS.Slots.get_known_clusters.bind
    @hook
    def get_known_clusters() -> set[str]:
        return {"known1", "known2"}

    request.addfinalizer(get_known_clusters.disable)

    with pytest.raises(
        ValueError,
        match=r"Cluster 'unknown' is not in the known clusters list: \['known1', 'known2'\]",
    ):
        SparkHDFS(cluster="unknown", spark=spark_mock)

    SparkHDFS(cluster="known1", spark=spark_mock)  # no exception


def test_spark_hdfs_known_normalize_cluster_name_hook(request, spark_mock):
    @SparkHDFS.Slots.normalize_cluster_name.bind
    @hook
    def normalize_cluster_name(cluster: str) -> str:
        return cluster.lower().replace("_", "-")

    request.addfinalizer(normalize_cluster_name.disable)

    assert SparkHDFS(cluster="rnd-dwh", spark=spark_mock).cluster == "rnd-dwh"
    assert SparkHDFS(cluster="rnd_dwh", spark=spark_mock).cluster == "rnd-dwh"
    assert SparkHDFS(cluster="RND-DWH", spark=spark_mock).cluster == "rnd-dwh"


def test_spark_hdfs_get_cluster_namenodes_hook(request, spark_mock):
    @SparkHDFS.Slots.get_cluster_namenodes.bind
    @hook
    def get_cluster_namenodes(cluster: str) -> set[str]:
        return {"some-node1.domain.com", "some-node2.domain.com"}

    request.addfinalizer(get_cluster_namenodes.disable)

    error_msg = (
        "Namenode 'unknown.domain.com' is not in the known nodes list of cluster 'rnd-dwh': "
        r"\['some-node1.domain.com', 'some-node2.domain.com'\]"
    )
    with pytest.raises(ValueError, match=error_msg):
        SparkHDFS(cluster="rnd-dwh", host="unknown.domain.com", spark=spark_mock)

    SparkHDFS(cluster="rnd-dwh", host="some-node1.domain.com", spark=spark_mock)  # no exception


def test_spark_hdfs_normalize_namenode_host_hook(request, spark_mock):
    @SparkHDFS.Slots.normalize_namenode_host.bind
    @hook
    def normalize_namenode_host(host: str, cluster: str) -> str:
        host = host.lower()
        if cluster == "rnd-dwh":
            if not host.endswith(".domain.com"):
                host += ".domain.com"
        return host

    request.addfinalizer(normalize_namenode_host.disable)

    assert SparkHDFS(host="some-node", cluster="rnd-dwh", spark=spark_mock).host == "some-node.domain.com"
    assert SparkHDFS(host="some-node", cluster="rnd-prod", spark=spark_mock).host == "some-node"


def test_spark_hdfs_get_ipc_port_hook(request, spark_mock):
    @SparkHDFS.Slots.get_ipc_port.bind
    @hook
    def get_ipc_port(cluster: str) -> int | None:
        if cluster == "rnd-dwh":
            return 9020
        return None

    request.addfinalizer(get_ipc_port.disable)

    assert SparkHDFS(cluster="rnd-dwh", spark=spark_mock).ipc_port == 9020
    assert SparkHDFS(cluster="rnd-prod", spark=spark_mock).ipc_port == 8020


def test_spark_hdfs_known_get_current(request, spark_mock):
    # no hooks bound to SparkHDFS.Slots.get_current_cluster
    error_msg = re.escape(
        "SparkHDFS.get_current() can be used only if there are some hooks bound to SparkHDFS.Slots.get_current_cluster",
    )
    with pytest.raises(RuntimeError, match=error_msg):
        SparkHDFS.get_current(spark=spark_mock)

    @SparkHDFS.Slots.get_current_cluster.bind
    @hook
    def get_current_cluster() -> str:
        return "rnd-dwh"

    request.addfinalizer(get_current_cluster.disable)

    conn = SparkHDFS.get_current(spark=spark_mock)
    assert conn.cluster == "rnd-dwh"
