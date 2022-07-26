import logging

import pytest

from onetl.connection import Hive
from onetl.connection.db_connection.hive import HiveWriteMode


@pytest.mark.parametrize(
    "sort_by",
    ["id_int", ["id_int", "hwm_int"]],
    ids=["sortBy as string.", "sortBy as List."],
)
def test_hive_options_sort_by_without_bucket_by(sort_by):
    with pytest.raises(ValueError, match="`sort_by` option can only be used with non-empty `bucket_by`"):
        Hive.Options(sortBy=sort_by)


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
        Hive.Options(**options)


@pytest.mark.parametrize("mode", ["static", "dynamic"])
def test_hive_options_unsupported_partition_overwrite(mode):
    error_msg = (
        "`partitionOverwriteMode` option should be replaced with mode='overwrite_partitions' or 'overwrite_table'"
    )

    with pytest.raises(ValueError, match=error_msg):
        Hive.Options(partitionOverwriteMode=mode)

    with pytest.raises(ValueError, match=error_msg):
        Hive.Options(partition_overwrite_mode=mode)


@pytest.mark.parametrize("insert_into", [True, False])
def test_hive_options_unsupported_insert_into(insert_into):
    error_msg = (
        "`insertInto` option was removed in onETL 0.4.0, "
        "now df.write.insertInto or df.write.saveAsTable is selected based on table existence"
    )

    with pytest.raises(ValueError, match=error_msg):
        Hive.Options(insert_into=insert_into)

    with pytest.raises(ValueError, match=error_msg):
        Hive.Options(insertInto=insert_into)


def test_hive_options_deprecated_mode_overwrite(caplog):
    warning_msg = (
        "Mode `overwrite` is deprecated since 0.4.0 and will be removed in 0.5.0, use `overwrite_partitions` instead"
    )

    with caplog.at_level(logging.INFO):
        options = Hive.Options(mode="overwrite")
        assert warning_msg in caplog.text

    assert options.mode == HiveWriteMode.OVERWRITE_PARTITIONS


def test_hive_missing_spark_arg():
    with pytest.raises(TypeError, match="missing 1 required positional argument: 'spark'"):
        Hive()  # noqa: F841
