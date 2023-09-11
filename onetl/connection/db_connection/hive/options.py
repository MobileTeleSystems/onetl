#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import warnings
from enum import Enum
from typing import List, Optional, Tuple, Union

from deprecated import deprecated
from pydantic import Field, root_validator, validator

from onetl.impl import GenericOptions


class HiveTableExistBehavior(str, Enum):
    APPEND = "append"
    IGNORE = "ignore"
    ERROR = "error"
    REPLACE_ENTIRE_TABLE = "replace_entire_table"
    REPLACE_OVERLAPPING_PARTITIONS = "replace_overlapping_partitions"

    def __str__(self):
        return str(self.value)

    @classmethod  # noqa: WPS120
    def _missing_(cls, value: object):  # noqa: WPS120
        if str(value) == "overwrite":
            warnings.warn(
                "Mode `overwrite` is deprecated since v0.4.0 and will be removed in v1.0.0. "
                "Use `replace_overlapping_partitions` instead",
                category=UserWarning,
                stacklevel=4,
            )
            return cls.REPLACE_OVERLAPPING_PARTITIONS

        if str(value) == "overwrite_partitions":
            warnings.warn(
                "Mode `overwrite_partitions` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `replace_overlapping_partitions` instead",
                category=UserWarning,
                stacklevel=4,
            )
            return cls.REPLACE_OVERLAPPING_PARTITIONS

        if str(value) == "overwrite_table":
            warnings.warn(
                "Mode `overwrite_table` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `replace_entire_table` instead",
                category=UserWarning,
                stacklevel=4,
            )
            return cls.REPLACE_ENTIRE_TABLE


class HiveWriteOptions(GenericOptions):
    """Hive source writing options.

    You can pass here key-value items which then will be converted to calls
    of :obj:`pyspark.sql.readwriter.DataFrameWriter` methods.

    For example, ``Hive.WriteOptions(if_exists="append", partitionBy="reg_id")`` will
    be converted to ``df.write.mode("append").partitionBy("reg_id")`` call, and so on.

    .. note::

        You can pass any method and its value
        `supported by Spark <https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html>`_,
        even if it is not mentioned in this documentation. **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version used. See link above.

    Examples
    --------

    Writing options initialization

    .. code:: python

        options = Hive.WriteOptions(
            if_exists="append",
            partitionBy="reg_id",
            someNewOption="value",
        )
    """

    class Config:
        known_options: frozenset = frozenset()
        extra = "allow"

    if_exists: HiveTableExistBehavior = Field(default=HiveTableExistBehavior.APPEND, alias="mode")
    """Behavior of writing data into existing table.

    Possible values:
        * ``append`` (default)
            Appends data into existing partition/table, or create partition/table if it does not exist.

            Same as Spark's ``df.write.insertInto(table, overwrite=False)``.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created using options provided by user (``format``, ``compression``, etc).

                * Table exists, but not partitioned, :obj:`~partition_by` is set
                    Data is appended to a table. Table is still not partitioned (DDL is unchanged).

                * Table exists and partitioned, but has different partitioning schema than :obj:`~partition_by`
                    Partition is created based on table's ``PARTITIONED BY (...)`` options.
                    Explicit :obj:`~partition_by` value is ignored.

                * Table exists and partitioned according :obj:`~partition_by`, but partition is present only in dataframe
                    Partition is created.

                * Table exists and partitioned according :obj:`~partition_by`, partition is present in both dataframe and table
                    Data is appended to existing partition.

                    .. warning::

                        This mode does not check whether table already contains
                        rows from dataframe, so duplicated rows can be created.

                        To implement deduplication, write data to staging table first,
                        and then perform some deduplication logic using :obj:`~sql`.

                * Table exists and partitioned according :obj:`~partition_by`, but partition is present only in table, not dataframe
                    Existing partition is left intact.

        * ``replace_overlapping_partitions``
            Overwrites data in the existing partition, or create partition/table if it does not exist.

            Same as Spark's ``df.write.insertInto(table, overwrite=True)`` +
            ``spark.sql.sources.partitionOverwriteMode=dynamic``.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created using options provided by user (``format``, ``compression``, etc).

                * Table exists, but not partitioned, :obj:`~partition_by` is set
                    Data is **overwritten in all the table**. Table is still not partitioned (DDL is unchanged).

                * Table exists and partitioned, but has different partitioning schema than :obj:`~partition_by`
                    Partition is created based on table's ``PARTITIONED BY (...)`` options.
                    Explicit :obj:`~partition_by` value is ignored.

                * Table exists and partitioned according :obj:`~partition_by`, but partition is present only in dataframe
                    Partition is created.

                * Table exists and partitioned according :obj:`~partition_by`, partition is present in both dataframe and table
                    Existing partition **replaced** with data from dataframe.

                * Table exists and partitioned according :obj:`~partition_by`, but partition is present only in table, not dataframe
                    Existing partition is left intact.

        * ``replace_entire_table``
            **Recreates table** (via ``DROP + CREATE``), **deleting all existing data**.
            **All existing partitions are dropped.**

            Same as Spark's ``df.write.saveAsTable(table, mode="overwrite")`` (NOT ``insertInto``)!

            .. warning::

                Table is recreated using options provided by user (``format``, ``compression``, etc)
                **instead of using original table options**. Be careful

        * ``ignore``
            Ignores the write operation if the table/partition already exists.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created using options provided by user (``format``, ``compression``, etc).

                * Table exists
                    If the table exists, **no further action is taken**. This is true whether or not new partition
                    values are present and whether the partitioning scheme differs or not

        * ``error``
            Raises an error if the table/partition already exists.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created using options provided by user (``format``, ``compression``, etc).

                * Table exists
                    If the table exists, **raises an error**. This is true whether or not new partition
                    values are present and whether the partitioning scheme differs or not


    .. note::

        Unlike using pure Spark, config option ``spark.sql.sources.partitionOverwriteMode``
        does not affect behavior.
    """

    format: str = "orc"
    """Format of files which should be used for storing table data.

    Examples: ``orc`` (default), ``parquet``, ``csv`` (NOT recommended)

    .. note::

        It's better to use column-based formats like ``orc`` or ``parquet``,
        not row-based (``csv``, ``json``)

    .. warning::

        Used **only** while **creating new table**, or in case of ``if_exists=recreate_entire_table``
    """

    partition_by: Optional[Union[List[str], str]] = Field(default=None, alias="partitionBy")
    """
    List of columns should be used for data partitioning. ``None`` means partitioning is disabled.

    Each partition is a folder which contains only files with the specific column value,
    like ``myschema.db/mytable/col1=value1``, ``myschema.db/mytable/col1=value2``, and so on.

    Multiple partitions columns means nested folder structure, like ``myschema.db/mytable/col1=val1/col2=val2``.

    If ``WHERE`` clause in the query contains expression like ``partition = value``,
    Spark will scan only files in a specific partition.

    Examples: ``reg_id`` or ``["reg_id", "business_dt"]``

    .. note::

        Values should be scalars (integers, strings),
        and either static (``countryId``) or incrementing (dates, years), with low
        number of distinct values.

        Columns like ``userId`` or ``datetime``/``timestamp`` should **NOT** be used for partitioning.

    .. warning::

        Used **only** while **creating new table**, or in case of ``if_exists=recreate_entire_table``
    """

    bucket_by: Optional[Tuple[int, Union[List[str], str]]] = Field(default=None, alias="bucketBy")  # noqa: WPS234
    """Number of buckets plus bucketing columns. ``None`` means bucketing is disabled.

    Each bucket is created as a set of files with name containing result of calculation ``hash(columns) mod num_buckets``.

    This allows to remove shuffle from queries containing ``GROUP BY`` or ``JOIN`` or using ``=`` / ``IN`` predicates
    on specific columns.

    Examples: ``(10, "user_id")``, ``(10, ["user_id", "user_phone"])``

    .. note::

        Bucketing should be used on columns containing a lot of unique values,
        like ``userId``.

        Columns like ``date`` should **NOT** be used for bucketing
        because of too low number of unique values.

    .. warning::

        It is recommended to use this option **ONLY** if you have a large table
        (hundreds of Gb or more), which is used mostly for JOINs with other tables,
        and you're inserting data using ``if_exists=overwrite_partitions`` or ``if_exists=recreate_entire_table``.

        Otherwise Spark will create a lot of small files
        (one file for each bucket and each executor), drastically **decreasing** HDFS performance.

    .. warning::

        Used **only** while **creating new table**, or in case of ``if_exists=recreate_entire_table``
    """

    sort_by: Optional[Union[List[str], str]] = Field(default=None, alias="sortBy")
    """Each file in a bucket will be sorted by these columns value. ``None`` means sorting is disabled.

    Examples: ``user_id`` or ``["user_id", "user_phone"]``

    .. note::

        Sorting columns should contain values which are used in ``ORDER BY`` clauses.

    .. warning::

        Could be used only with :obj:`~bucket_by` option

    .. warning::

        Used **only** while **creating new table**, or in case of ``if_exists=recreate_entire_table``
    """

    compression: Optional[str] = None
    """Compressing algorithm which should be used for compressing created files in HDFS.
    ``None`` means compression is disabled.

    Examples: ``snappy``, ``zlib``

    .. warning::

        Used **only** while **creating new table**, or in case of ``if_exists=recreate_entire_table``
    """

    @validator("sort_by")
    def _sort_by_cannot_be_used_without_bucket_by(cls, sort_by, values):
        options = values.copy()
        bucket_by = options.pop("bucket_by", None)
        if sort_by and not bucket_by:
            raise ValueError("`sort_by` option can only be used with non-empty `bucket_by`")

        return sort_by

    @root_validator
    def _partition_overwrite_mode_is_not_allowed(cls, values):
        partition_overwrite_mode = values.get("partitionOverwriteMode") or values.get("partition_overwrite_mode")
        if partition_overwrite_mode:
            if partition_overwrite_mode == "static":
                recommend_mode = "replace_entire_table"
            else:
                recommend_mode = "replace_overlapping_partitions"
            raise ValueError(
                f"`partitionOverwriteMode` option should be replaced with if_exists='{recommend_mode}'",
            )

        if values.get("insert_into") is not None or values.get("insertInto") is not None:
            raise ValueError(
                "`insertInto` option was removed in onETL 0.4.0, "
                "now df.write.insertInto or df.write.saveAsTable is selected based on table existence",
            )

        return values

    @root_validator(pre=True)
    def _mode_is_deprecated(cls, values):
        if "mode" in values:
            warnings.warn(
                "Option `Hive.WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `Hive.WriteOptions(if_exists=...)` instead",
                category=UserWarning,
                stacklevel=3,
            )
        return values


@deprecated(
    version="0.5.0",
    reason="Please use 'WriteOptions' class instead. Will be removed in v1.0.0",
    action="always",
    category=UserWarning,
)
class HiveLegacyOptions(HiveWriteOptions):
    pass
