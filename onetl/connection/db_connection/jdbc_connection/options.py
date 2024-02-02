# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings
from enum import Enum
from typing import Optional

from deprecated import deprecated
from pydantic import Field, PositiveInt, root_validator

from onetl._internal import to_camel
from onetl.connection.db_connection.jdbc_mixin.options import JDBCOptions

# options from spark.read.jdbc which are populated by JDBCConnection methods
GENERIC_PROHIBITED_OPTIONS = frozenset(
    (
        "table",
        "dbtable",
        "query",
        "properties",
    ),
)

READ_WRITE_OPTIONS = frozenset(
    (
        "keytab",
        "principal",
        "refreshKrb5Config",
        "connectionProvider",
    ),
)

WRITE_OPTIONS = frozenset(
    (
        "mode",
        "column",  # in some part of Spark source code option 'partitionColumn' is called just 'column'
        "batchsize",
        "isolationLevel",
        "isolation_level",
        "truncate",
        "cascadeTruncate",
        "createTableOptions",
        "createTableColumnTypes",
        "createTableColumnTypes",
    ),
)

READ_OPTIONS = frozenset(
    (
        "column",  # in some part of Spark source code option 'partitionColumn' is called just 'column'
        "partitionColumn",
        "partition_column",
        "lowerBound",
        "lower_bound",
        "upperBound",
        "upper_bound",
        "numPartitions",
        "num_partitions",
        "fetchsize",
        "sessionInitStatement",
        "session_init_statement",
        "customSchema",
        "pushDownPredicate",
        "pushDownAggregate",
        "pushDownLimit",
        "pushDownTableSample",
        "predicates",
    ),
)


class JDBCTableExistBehavior(str, Enum):
    APPEND = "append"
    IGNORE = "ignore"
    ERROR = "error"
    REPLACE_ENTIRE_TABLE = "replace_entire_table"

    def __str__(self) -> str:
        return str(self.value)

    @classmethod  # noqa: WPS120
    def _missing_(cls, value: object):  # noqa: WPS120
        if str(value) == "overwrite":
            warnings.warn(
                "Mode `overwrite` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `replace_entire_table` instead",
                category=UserWarning,
                stacklevel=4,
            )
            return cls.REPLACE_ENTIRE_TABLE


class JDBCPartitioningMode(str, Enum):
    RANGE = "range"
    HASH = "hash"
    MOD = "mod"

    def __str__(self):
        return str(self.value)


class JDBCReadOptions(JDBCOptions):
    """Spark JDBC reading options.

    .. note ::

        You can pass any value
        `supported by Spark <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>`_,
        even if it is not mentioned in this documentation. **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version. See link above.

    Examples
    --------

    Read options initialization

    .. code:: python

        options = JDBC.ReadOptions(
            partitionColumn="reg_id",
            numPartitions=10,
            lowerBound=0,
            upperBound=1000,
            someNewOption="value",
        )
    """

    class Config:
        known_options = READ_OPTIONS | READ_WRITE_OPTIONS
        prohibited_options = JDBCOptions.Config.prohibited_options | GENERIC_PROHIBITED_OPTIONS | WRITE_OPTIONS
        alias_generator = to_camel

    # Options in DataFrameWriter.jdbc() method
    partition_column: Optional[str] = None
    """Column used to parallelize reading from a table.

    .. warning::
        It is highly recommended to use primary key, or at least a column with an index
        to avoid performance issues.

    .. note::
        Column type depends on :obj:`~partitioning_mode`.

        * ``partitioning_mode="range"`` requires column to be an integer or date (can be NULL, but not recommended).
        * ``partitioning_mode="hash"`` requires column to be an string (NOT NULL).
        * ``partitioning_mode="mod"`` requires column to be an integer (NOT NULL).


    See documentation for :obj:`~partitioning_mode` for more details"""

    num_partitions: PositiveInt = 1
    """Number of jobs created by Spark to read the table content in parallel.
    See documentation for :obj:`~partitioning_mode` for more details"""

    lower_bound: Optional[int] = None
    """See documentation for :obj:`~partitioning_mode` for more details"""  # noqa: WPS322

    upper_bound: Optional[int] = None
    """See documentation for :obj:`~partitioning_mode` for more details"""  # noqa: WPS322

    session_init_statement: Optional[str] = None
    '''After each database session is opened to the remote DB and before starting to read data,
    this option executes a custom SQL statement (or a PL/SQL block).

    Use this to implement session initialization code.

    Example:

    .. code:: python

        sessionInitStatement = """
            BEGIN
                execute immediate
                'alter session set "_serial_direct_read"=true';
            END;
        """
    '''

    fetchsize: int = 100_000
    """Fetch N rows from an opened cursor per one read round.

    Tuning this option can influence performance of reading.

    .. warning::

        Default value is different from Spark.

        Spark uses driver's own value, and it may be different in different drivers,
        and even versions of the same driver. For example, Oracle has
        default ``fetchsize=10``, which is absolutely not usable.

        Thus we've overridden default value with ``100_000``, which should increase reading performance.
    """

    partitioning_mode: JDBCPartitioningMode = JDBCPartitioningMode.RANGE
    """Defines how Spark will parallelize reading from table.

    Possible values:

    * ``range`` (default)
        Allocate each executor a range of values from column passed into :obj:`~partition_column`.

        Spark generates for each executor an SQL query like:

        Executor 1:

        .. code:: sql

            SELECT ... FROM table
            WHERE (partition_column >= lowerBound
                    OR partition_column IS NULL)
            AND partition_column < (lower_bound + stride)

        Executor 2:

        .. code:: sql

            SELECT ... FROM table
            WHERE partition_column >= (lower_bound + stride)
            AND partition_column < (lower_bound + 2 * stride)

        ...

        Executor N:

        .. code:: sql

            SELECT ... FROM table
            WHERE partition_column >= (lower_bound + (N-1) * stride)
            AND partition_column <= upper_bound

        Where ``stride=(upper_bound - lower_bound) / num_partitions``.

        .. note::

            :obj:`~lower_bound`, :obj:`~upper_bound` and :obj:`~num_partitions` are used just to
            calculate the partition stride, **NOT** for filtering the rows in table.
            So all rows in the table will be returned (unlike *Incremental* :ref:`strategy`).

        .. note::

            All queries are executed in parallel. To execute them sequentially, use *Batch* :ref:`strategy`.

    * ``hash``
        Allocate each executor a set of values based on hash of the :obj:`~partition_column` column.

        Spark generates for each executor an SQL query like:

        Executor 1:

        .. code:: sql

            SELECT ... FROM table
            WHERE (some_hash(partition_column) mod num_partitions) = 0 -- lower_bound

        Executor 2:

        .. code:: sql

            SELECT ... FROM table
            WHERE (some_hash(partition_column) mod num_partitions) = 1 -- lower_bound + 1

        ...

        Executor N:

        .. code:: sql

            SELECT ... FROM table
            WHERE (some_hash(partition_column) mod num_partitions) = num_partitions-1 -- upper_bound

        .. note::

            The hash function implementation depends on RDBMS. It can be ``MD5`` or any other fast hash function,
            or expression based on this function call.

    * ``mod``
        Allocate each executor a set of values based on modulus of the :obj:`~partition_column` column.

        Spark generates for each executor an SQL query like:

        Executor 1:

        .. code:: sql

            SELECT ... FROM table
            WHERE (partition_column mod num_partitions) = 0 -- lower_bound

        Executor 2:

        .. code:: sql

            SELECT ... FROM table
            WHERE (partition_column mod num_partitions) = 1 -- lower_bound + 1

        Executor N:

        .. code:: sql

            SELECT ... FROM table
            WHERE (partition_column mod num_partitions) = num_partitions-1 -- upper_bound

    Examples
    --------

    Read data in 10 parallel jobs by range of values in ``id_column`` column:

    .. code:: python

        JDBC.ReadOptions(
            partitioning_mode="range",  # default mode, can be omitted
            partition_column="id_column",
            num_partitions=10,
            # if you're using DBReader, options below can be omitted
            # because they are calculated by automatically as
            # MIN and MAX values of `partition_column`
            lower_bound=0,
            upper_bound=100_000,
        )

    Read data in 10 parallel jobs by hash of values in ``some_column`` column:

    .. code:: python

        JDBC.ReadOptions(
            partitioning_mode="hash",
            partition_column="some_column",
            num_partitions=10,
            # lower_bound and upper_bound are automatically set to `0` and `9`
        )

    Read data in 10 parallel jobs by modulus of values in ``id_column`` column:

    .. code:: python

        JDBC.ReadOptions(
            partitioning_mode="mod",
            partition_column="id_column",
            num_partitions=10,
            # lower_bound and upper_bound are automatically set to `0` and `9`
        )
    """

    @root_validator
    def _partitioning_mode_actions(cls, values):
        mode = values["partitioning_mode"]
        num_partitions = values.get("num_partitions")
        partition_column = values.get("partition_column")
        lower_bound = values.get("lower_bound")
        upper_bound = values.get("upper_bound")

        if not partition_column:
            if num_partitions == 1:
                return values

            raise ValueError("You should set partition_column to enable partitioning")

        elif num_partitions == 1:
            raise ValueError("You should set num_partitions > 1 to enable partitioning")

        if mode == JDBCPartitioningMode.RANGE:
            return values

        values["lower_bound"] = lower_bound if lower_bound is not None else 0
        values["upper_bound"] = upper_bound if upper_bound is not None else num_partitions
        return values


class JDBCWriteOptions(JDBCOptions):
    """Spark JDBC writing options.

    .. note ::

        You can pass any value
        `supported by Spark <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>`_,
        even if it is not mentioned in this documentation. **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version. See link above.

    Examples
    --------

    Write options initialization

    .. code:: python

        options = JDBC.WriteOptions(if_exists="append", batchsize=20_000, someNewOption="value")
    """

    class Config:
        known_options = WRITE_OPTIONS | READ_WRITE_OPTIONS
        prohibited_options = JDBCOptions.Config.prohibited_options | GENERIC_PROHIBITED_OPTIONS | READ_OPTIONS
        alias_generator = to_camel

    if_exists: JDBCTableExistBehavior = Field(default=JDBCTableExistBehavior.APPEND, alias="mode")
    """Behavior of writing data into existing table.

    Possible values:
        * ``append`` (default)
            Adds new rows into existing table.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created using options provided by user
                    (``createTableOptions``, ``createTableColumnTypes``, etc).

                * Table exists
                    Data is appended to a table. Table has the same DDL as before writing data

                    .. warning::

                        This mode does not check whether table already contains
                        rows from dataframe, so duplicated rows can be created.

                        Also Spark does not support passing custom options to
                        insert statement, like ``ON CONFLICT``, so don't try to
                        implement deduplication using unique indexes or constraints.

                        Instead, write to staging table and perform deduplication
                        using :obj:`~execute` method.

        * ``replace_entire_table``
            **Table is dropped and then created, or truncated**.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created using options provided by user
                    (``createTableOptions``, ``createTableColumnTypes``, etc).

                * Table exists
                    Table content is replaced with dataframe content.

                    After writing completed, target table could either have the same DDL as
                    before writing data (``truncate=True``), or can be recreated (``truncate=False``
                    or source does not support truncation).

        * ``ignore``
            Ignores the write operation if the table already exists.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created using options provided by user
                    (``createTableOptions``, ``createTableColumnTypes``, etc).

                * Table exists
                    The write operation is ignored, and no data is written to the table.

        * ``error``
            Raises an error if the table already exists.

            .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created using options provided by user
                    (``createTableOptions``, ``createTableColumnTypes``, etc).

                * Table exists
                    An error is raised, and no data is written to the table.

    """

    batchsize: int = 20_000
    """How many rows can be inserted per round trip.

    Tuning this option can influence performance of writing.

    .. warning::

        Default value is different from Spark.

        Spark uses quite small value ``1000``, which is absolutely not usable
        in BigData world.

        Thus we've overridden default value with ``20_000``,
        which should increase writing performance.

        You can increase it even more, up to ``50_000``,
        but it depends on your database load and number of columns in the row.
        Higher values does not increase performance.
    """

    isolation_level: str = "READ_UNCOMMITTED"
    """The transaction isolation level, which applies to current connection.

    Possible values:
        * ``NONE`` (as string, not Python's ``None``)
        * ``READ_COMMITTED``
        * ``READ_UNCOMMITTED``
        * ``REPEATABLE_READ``
        * ``SERIALIZABLE``

    Values correspond to transaction isolation levels defined by JDBC standard.
    Please refer the documentation for
    `java.sql.Connection <https://docs.oracle.com/javase/8/docs/api/java/sql/Connection.html>`_.
    """

    @root_validator(pre=True)
    def _mode_is_deprecated(cls, values):
        if "mode" in values:
            warnings.warn(
                "Option `WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `WriteOptions(if_exists=...)` instead",
                category=UserWarning,
                stacklevel=3,
            )
        return values


@deprecated(
    version="0.5.0",
    reason="Please use 'ReadOptions' or 'WriteOptions' class instead. Will be removed in v1.0.0",
    action="always",
    category=UserWarning,
)
class JDBCLegacyOptions(JDBCReadOptions, JDBCWriteOptions):
    class Config:
        prohibited_options = JDBCOptions.Config.prohibited_options
