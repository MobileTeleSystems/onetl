# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from enum import Enum

from onetl._util.alias import avoid_alias

try:
    from pydantic.v1 import Field
except (ImportError, AttributeError):
    from pydantic import Field  # type: ignore[no-redef, assignment]

from onetl.impl import GenericOptions

PROHIBITED_OPTIONS = frozenset(
    (
        # These options should be passed in Spark session config
        "spark.*",
    ),
)


class IcebergTableExistBehavior(str, Enum):
    APPEND = "append"
    IGNORE = "ignore"
    ERROR = "error"
    REPLACE_ENTIRE_TABLE = "replace_entire_table"
    REPLACE_OVERLAPPING_PARTITIONS = "replace_overlapping_partitions"

    def __str__(self):
        return str(self.value)


class IcebergReadOptions(GenericOptions):
    """Iceberg source reading options."""

    class Config:
        extra = "allow"
        known_options: frozenset = frozenset()
        prohibited_options = PROHIBITED_OPTIONS


class IcebergWriteOptions(GenericOptions):
    """Iceberg source writing options."""

    class Config:
        extra = "allow"
        known_options: frozenset = frozenset()
        prohibited_options = PROHIBITED_OPTIONS

    if_exists: IcebergTableExistBehavior = Field(  # type: ignore[literal-required]
        default=IcebergTableExistBehavior.APPEND,
        alias=avoid_alias("mode"),
    )
    """Behavior of writing data into existing table.

    Possible values:
        * ``append`` (default)
            Appends data into existing table, or create table if it does not exist.

            Same as Spark's ``df.writeTo(table).using("iceberg").append()``.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created.

                * Table exists and not partitioned
                    Data is appended to a table. Table DDL (including partition spec) is unchanged.

                * Table exists and partitioned
                    If a partition is present only in dataframe
                        Partition is created.
                    If a partition is present in both dataframe and table
                        Data is appended to existing partition.

                    .. warning::

                        This mode does not check whether table already contains
                        rows from dataframe, so duplicated rows can be created.

                        To implement deduplication, write data to staging table first,
                        and then perform some deduplication logic using :obj:`~sql`.

                * Table exists and partitioned, but some partitions are present only in table, not dataframe
                    Existing partitions are left intact.

        * ``replace_overlapping_partitions``
            Overwrites data in the existing partitions, or create table if it does not exist.

            Same as Spark's ``df.writeTo(table).using("iceberg").overwritePartitions()``

            .. DANGER::

                This mode does make sense **ONLY** if the table is partitioned.
                **IF NOT, YOU'LL LOSE YOUR DATA!**

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created.

                * Table exists and not partitioned
                    Data is **overwritten in all the table**. Table DDL (including partition spec) is unchanged.

                * Table exists and partitioned
                    If a partition is present only in dataframe
                        Partition is created.
                    If a partition is present in both dataframe and table
                        Existing partition **replaced** with data from dataframe.
                    If a partition is present only in table, not dataframe
                        Existing partition is left intact.

        * ``replace_entire_table``
            **Recreates table** (via ``DROP + CREATE``), **deleting all existing data**.
            **All existing partitions are dropped.**

            Same as Spark's ``df.writeTo(table).createOrReplace()``

            .. warning::

                Table is recreated
                **instead of using original table options**. Be careful

        * ``ignore``
            Ignores the write operation if the table already exists.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created.

                * Table exists
                    If the table exists, **no further action is taken**.

        * ``error``
            Raises an error if the table already exists.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created.

                * Table exists
                    If the table exists, **raises an error**.
    """
