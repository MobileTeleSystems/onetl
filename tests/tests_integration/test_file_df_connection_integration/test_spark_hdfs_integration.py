from __future__ import annotations

import logging

import pytest

from onetl.hooks import hook

pytestmark = [pytest.mark.hdfs, pytest.mark.file_df_connection, pytest.mark.connection]


def test_spark_hdfs_check(hdfs_file_df_connection, caplog):
    hdfs = hdfs_file_df_connection

    with caplog.at_level(logging.INFO):
        assert hdfs.check() == hdfs

    assert "|SparkHDFS|" in caplog.text
    assert f"cluster = '{hdfs.cluster}'" in caplog.text
    assert f"host = '{hdfs.host}'" in caplog.text
    assert f"ipc_port = {hdfs.ipc_port}" in caplog.text

    assert "Connection is available." in caplog.text


def test_spark_hdfs_file_connection_check_failed(spark):
    from onetl.connection import SparkHDFS

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        SparkHDFS(
            cluster="rnd-dwh",
            host="hive1",
            port=1234,
            spark=spark,
        ).check()


def test_spark_hdfs_file_connection_check_with_hooks(spark, request, hdfs_server):
    from onetl.connection import SparkHDFS

    @SparkHDFS.Slots.is_namenode_active.bind
    @hook
    def is_namenode_active(host: str, cluster: str) -> bool:
        return host == hdfs_server.host

    request.addfinalizer(is_namenode_active.disable)

    # no exception
    SparkHDFS(
        cluster="rnd-dwh",
        host=hdfs_server.host,
        port=hdfs_server.ipc_port,
        spark=spark,
    ).check()

    with pytest.raises(
        RuntimeError,
        match="Host 'some-node2.domain.com' is not an active namenode of cluster 'rnd-dwh'",
    ):
        SparkHDFS(
            cluster="rnd-dwh",
            host="some-node2.domain.com",
            spark=spark,
        ).check()

    with pytest.raises(RuntimeError, match="Cannot get list of namenodes for a cluster 'rnd-dwh'"):
        SparkHDFS(cluster="rnd-dwh", spark=spark).check()

    @SparkHDFS.Slots.get_cluster_namenodes.bind
    @hook
    def get_cluster_namenodes(cluster: str) -> set[str]:
        if cluster == "rnd-dwh":
            return {hdfs_server.host}
        return {"some-node1.domain.com"}

    request.addfinalizer(get_cluster_namenodes.disable)

    @SparkHDFS.Slots.get_ipc_port.bind
    @hook
    def get_ipc_port(cluster: str) -> int | None:
        if cluster == "rnd-dwh":
            return hdfs_server.ipc_port
        return None

    request.addfinalizer(get_ipc_port.disable)

    # no exception
    assert SparkHDFS(cluster="rnd-dwh", spark=spark).check()

    with pytest.raises(RuntimeError, match="Cannot detect active namenode for cluster 'rnd-prod'"):
        SparkHDFS(cluster="rnd-prod", spark=spark).check()

    @SparkHDFS.Slots.get_current_cluster.bind
    @hook
    def get_current_cluster() -> str:
        return "rnd-dwh"

    request.addfinalizer(get_current_cluster.disable)
    hdfs = SparkHDFS.get_current(spark=spark)
    assert hdfs.cluster == "rnd-dwh"
    assert hdfs.check()
