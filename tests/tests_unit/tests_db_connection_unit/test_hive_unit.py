from __future__ import annotations

import logging
import re

import pytest

from onetl.connection import Hive
from onetl.connection.db_connection.hive import HiveWriteMode
from onetl.hooks import hook


def test_hive_missing_args(spark_mock):
    # no spark
    with pytest.raises(ValueError, match="field required"):
        Hive()

    # no cluster
    with pytest.raises(ValueError, match="field required"):
        Hive(spark=spark_mock)


def test_hive_instance_url(spark_mock):
    hive = Hive(cluster="some-cluster", spark=spark_mock)
    assert hive.instance_url == "some-cluster"


def test_hive_get_known_clusters_hook(request, spark_mock):
    # no exception
    Hive(cluster="unknown", spark=spark_mock)

    @Hive.slots.get_known_clusters.bind
    @hook
    def get_known_clusters() -> set[str]:
        return {"known1", "known2"}

    request.addfinalizer(get_known_clusters.disable)

    with pytest.raises(ValueError, match="Cluster 'unknown' is not in the known clusters list: 'known1', 'known2'"):
        Hive(cluster="unknown", spark=spark_mock)

    Hive(cluster="known1", spark=spark_mock)  # no exception


def test_hive_known_normalize_cluster_name_hook(request, spark_mock):
    @Hive.slots.normalize_cluster_name.bind
    @hook
    def normalize_cluster_name(cluster: str) -> str:
        return cluster.lower().replace("_", "-")

    request.addfinalizer(normalize_cluster_name.disable)

    assert Hive(cluster="rnd-dwh", spark=spark_mock).cluster == "rnd-dwh"
    assert Hive(cluster="rnd_dwh", spark=spark_mock).cluster == "rnd-dwh"
    assert Hive(cluster="RND-DWH", spark=spark_mock).cluster == "rnd-dwh"


def test_hive_known_get_current_cluster_hook(request, spark_mock, mocker):
    mocker.patch.object(Hive, "_execute_sql", return_value=None)

    # no exception
    Hive(cluster="rnd-prod", spark=spark_mock).check()
    Hive(cluster="rnd-dwh", spark=spark_mock).check()

    @Hive.slots.get_current_cluster.bind
    @hook
    def get_current_cluster() -> str:
        return "rnd-dwh"

    request.addfinalizer(get_current_cluster.disable)

    with pytest.raises(ValueError, match="You can connect to a Hive cluster only from the same cluster"):
        Hive(cluster="rnd-prod", spark=spark_mock).check()

    # same cluster as in get_current_cluster
    Hive(cluster="rnd-dwh", spark=spark_mock).check()


def test_hive_known_get_current(request, spark_mock):
    # no hooks bound to Hive.slots.get_current_cluster
    error_msg = re.escape(
        "Hive.get_current() can be used only if there are some hooks bound to Hive.slots.get_current_cluster",
    )
    with pytest.raises(RuntimeError, match=error_msg):
        Hive.get_current(spark=spark_mock)

    @Hive.slots.get_current_cluster.bind
    @hook
    def get_current_cluster() -> str:
        return "rnd-dwh"

    request.addfinalizer(get_current_cluster.disable)

    assert Hive.get_current(spark=spark_mock).cluster == "rnd-dwh"


def test_hive_old_options_deprecated():
    warning_msg = "Please use 'WriteOptions' class instead. Will be removed in v1.0.0"
    with pytest.deprecated_call(match=warning_msg):
        options = Hive.Options(some="value")

    assert options.some == "value"


@pytest.mark.parametrize(
    "sort_by",
    ["id_int", ["id_int", "hwm_int"]],
    ids=["sortBy as string.", "sortBy as List."],
)
def test_hive_write_options_sort_by_without_bucket_by(sort_by):
    with pytest.raises(ValueError, match="`sort_by` option can only be used with non-empty `bucket_by`"):
        Hive.WriteOptions(sortBy=sort_by)


@pytest.mark.parametrize(
    "options",
    [
        # disallowed modes
        {"mode": "error"},
        {"mode": "ignore"},
    ],
)
def test_hive_options_unsupported_modes(options):
    with pytest.raises(ValueError, match="value is not a valid enumeration member"):
        Hive.WriteOptions(**options)


@pytest.mark.parametrize("mode", ["static", "dynamic"])
def test_hive_write_options_unsupported_partition_overwrite(mode):
    error_msg = (
        "`partitionOverwriteMode` option should be replaced with mode='overwrite_partitions' or 'overwrite_table'"
    )

    with pytest.raises(ValueError, match=error_msg):
        Hive.WriteOptions(partitionOverwriteMode=mode)

    with pytest.raises(ValueError, match=error_msg):
        Hive.WriteOptions(partition_overwrite_mode=mode)


@pytest.mark.parametrize("insert_into", [True, False])
def test_hive_write_options_unsupported_insert_into(insert_into):
    error_msg = (
        "`insertInto` option was removed in onETL 0.4.0, "
        "now df.write.insertInto or df.write.saveAsTable is selected based on table existence"
    )

    with pytest.raises(ValueError, match=error_msg):
        Hive.WriteOptions(insert_into=insert_into)

    with pytest.raises(ValueError, match=error_msg):
        Hive.WriteOptions(insertInto=insert_into)


def test_hive_write_options_deprecated_mode_overwrite(caplog):
    warning_msg = (
        "Mode `overwrite` is deprecated since v0.4.0 and will be removed in v1.0.0, use `overwrite_partitions` instead"
    )

    with caplog.at_level(logging.INFO):
        options = Hive.WriteOptions(mode="overwrite")
        assert warning_msg in caplog.text

    assert options.mode == HiveWriteMode.OVERWRITE_PARTITIONS
