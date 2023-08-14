from __future__ import annotations

import re

import pytest

from onetl.connection import Hive
from onetl.connection.db_connection.hive import HiveTableExistBehavior
from onetl.hooks import hook

pytestmark = [pytest.mark.hive, pytest.mark.db_connection, pytest.mark.connection]


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

    @Hive.Slots.get_known_clusters.bind
    @hook
    def get_known_clusters() -> set[str]:
        return {"known1", "known2"}

    request.addfinalizer(get_known_clusters.disable)

    with pytest.raises(
        ValueError,
        match=r"Cluster 'unknown' is not in the known clusters list: \['known1', 'known2'\]",
    ):
        Hive(cluster="unknown", spark=spark_mock)

    Hive(cluster="known1", spark=spark_mock)  # no exception


def test_hive_known_normalize_cluster_name_hook(request, spark_mock):
    @Hive.Slots.normalize_cluster_name.bind
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

    @Hive.Slots.get_current_cluster.bind
    @hook
    def get_current_cluster() -> str:
        return "rnd-dwh"

    request.addfinalizer(get_current_cluster.disable)

    with pytest.raises(ValueError, match="You can connect to a Hive cluster only from the same cluster"):
        Hive(cluster="rnd-prod", spark=spark_mock).check()

    # same cluster as in get_current_cluster
    Hive(cluster="rnd-dwh", spark=spark_mock).check()


def test_hive_known_get_current(request, spark_mock):
    # no hooks bound to Hive.Slots.get_current_cluster
    error_msg = re.escape(
        "Hive.get_current() can be used only if there are some hooks bound to Hive.Slots.get_current_cluster",
    )
    with pytest.raises(RuntimeError, match=error_msg):
        Hive.get_current(spark=spark_mock)

    @Hive.Slots.get_current_cluster.bind
    @hook
    def get_current_cluster() -> str:
        return "rnd-dwh"

    request.addfinalizer(get_current_cluster.disable)

    assert Hive.get_current(spark=spark_mock).cluster == "rnd-dwh"


def test_hive_old_options_deprecated():
    warning_msg = "Please use 'WriteOptions' class instead. Will be removed in v1.0.0"
    with pytest.warns(UserWarning, match=warning_msg):
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
    "mode, recommended",
    [
        ("dynamic", "replace_overlapping_partitions"),
        ("static", "replace_entire_table"),
    ],
)
def test_hive_write_options_unsupported_partition_overwrite(mode, recommended):
    error_msg = f"`partitionOverwriteMode` option should be replaced with if_exists='{recommended}'"

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


@pytest.mark.parametrize(
    "options, value",
    [
        ({}, HiveTableExistBehavior.APPEND),
        ({"if_exists": "append"}, HiveTableExistBehavior.APPEND),
        ({"if_exists": "replace_overlapping_partitions"}, HiveTableExistBehavior.REPLACE_OVERLAPPING_PARTITIONS),
        ({"if_exists": "replace_entire_table"}, HiveTableExistBehavior.REPLACE_ENTIRE_TABLE),
    ],
)
def test_hive_write_options_if_exists(options, value):
    assert Hive.WriteOptions(**options).if_exists == value


@pytest.mark.parametrize(
    "options, value, message",
    [
        (
            {"mode": "append"},
            HiveTableExistBehavior.APPEND,
            "Option `Hive.WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `Hive.WriteOptions(if_exists=...)` instead",
        ),
        (
            {"mode": "replace_overlapping_partitions"},
            HiveTableExistBehavior.REPLACE_OVERLAPPING_PARTITIONS,
            "Option `Hive.WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `Hive.WriteOptions(if_exists=...)` instead",
        ),
        (
            {"mode": "replace_entire_table"},
            HiveTableExistBehavior.REPLACE_ENTIRE_TABLE,
            "Option `Hive.WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `Hive.WriteOptions(if_exists=...)` instead",
        ),
        (
            {"mode": "overwrite"},
            HiveTableExistBehavior.REPLACE_OVERLAPPING_PARTITIONS,
            "Mode `overwrite` is deprecated since v0.4.0 and will be removed in v1.0.0. "
            "Use `replace_overlapping_partitions` instead",
        ),
        (
            {"mode": "overwrite_partitions"},
            HiveTableExistBehavior.REPLACE_OVERLAPPING_PARTITIONS,
            "Mode `overwrite_partitions` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `replace_overlapping_partitions` instead",
        ),
        (
            {"mode": "overwrite_table"},
            HiveTableExistBehavior.REPLACE_ENTIRE_TABLE,
            "Mode `overwrite_table` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `replace_entire_table` instead",
        ),
    ],
)
def test_hive_write_options_mode_deprecated(options, value, message):
    with pytest.warns(UserWarning, match=re.escape(message)):
        options = Hive.WriteOptions(**options)
        assert options.if_exists == value


@pytest.mark.parametrize(
    "options",
    [
        # disallowed modes
        {"mode": "error"},
        {"mode": "ignore"},
        # wrong mode
        {"mode": "wrong_mode"},
    ],
)
def test_hive_write_options_mode_unsupported(options):
    with pytest.raises(ValueError, match="value is not a valid enumeration member"):
        Hive.WriteOptions(**options)
