# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import warnings
from enum import Enum

from pydantic import ConfigDict, Field, PositiveInt, model_validator
from typing_extensions import deprecated

from onetl._util.alias import avoid_alias
from onetl.connection.db_connection.jdbc_mixin.options import JDBCFetchOptions
from onetl.impl import GenericOptions

# options from spark.read.jdbc which are populated by JDBCConnection methods
GENERIC_PROHIBITED_OPTIONS = frozenset(
    (
        "user",
        "password",
        "driver",
        "url",
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

    @classmethod
    def _missing_(cls, value: object):
        if str(value) == "overwrite":
            warnings.warn(
                "Mode `overwrite` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `replace_entire_table` instead",
                category=UserWarning,
                stacklevel=4,
            )
            return cls.REPLACE_ENTIRE_TABLE
        return None


class JDBCPartitioningMode(str, Enum):
    RANGE = "range"
    HASH = "hash"
    MOD = "mod"

    def __str__(self):
        return str(self.value)


class JDBCReadOptions(JDBCFetchOptions):
    """Spark JDBC reading options.

    !!! success "Added in 0.5.0"
        Replace `SomeDB.Options` → `SomeDB.ReadOptions`

    Examples
    --------

    !!! note

        You can pass any value
        [supported by Spark](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html),
        even if it is not mentioned in this documentation. **Option names should be in** `camelCase`!

        The set of supported options depends on Spark version.

    ```python
    from onetl.connection import SomeDB

    options = SomeDB.ReadOptions(
        partitioning_mode="range",
        partitionColumn="reg_id",
        numPartitions=10,
        customSparkOption="value",
    )
    ```
    """

    model_config = ConfigDict(
        known_options=READ_OPTIONS | READ_WRITE_OPTIONS,  # type: ignore[typeddict-unknown-key]
        prohibited_options=GENERIC_PROHIBITED_OPTIONS | WRITE_OPTIONS,  # type: ignore[typeddict-unknown-key]
        extra="allow",
    )

    # Options in DataFrameWriter.jdbc() method
    partition_column: str | None = Field(default=None, alias="partitionColumn")
    """Column used to parallelize reading from a table.

    !!! warning
        It is highly recommended to use primary key, or column with an index
        to avoid performance issues.

    !!! note
        Column type depends on [partitioning_mode][].

        * `partitioning_mode="range"` requires column to be an integer,
          date or timestamp (can be NULL, but not recommended).
        * `partitioning_mode="hash"` accepts any column type (NOT NULL).
        * `partitioning_mode="mod"` requires column to be an integer (NOT NULL).

    See documentation for [partitioning_mode][] for more details"""

    num_partitions: PositiveInt = Field(default=1, alias="numPartitions")
    """Number of jobs created by Spark to read the table content in parallel.
    See documentation for [partitioning_mode][] for more details"""

    lower_bound: int | None = Field(default=None, alias="lowerBound")
    """See documentation for [partitioning_mode][] for more details"""

    upper_bound: int | None = Field(default=None, alias="upperBound")
    """See documentation for [partitioning_mode][] for more details"""

    session_init_statement: str | None = Field(default=None, alias="sessionInitStatement")
    '''After each database session is opened to the remote DB and before starting to read data,
    this option executes a custom SQL statement (or a PL/SQL block).

    Use this to implement session initialization code.

    Example:

    ```python
    sessionInitStatement = """
        BEGIN
            execute immediate
            'alter session set "_serial_direct_read"=true';
        END;
    """
    ```
    '''

    query_timeout: int | None = Field(default=None, alias="queryTimeout")
    """The number of seconds the driver will wait for a statement to execute.
    Zero means there is no limit.

    This option depends on driver implementation,
    some drivers can check the timeout of each query instead of an entire JDBC batch.
    """

    fetchsize: int = 100_000
    """Fetch N rows from an opened cursor per one read round.

    Tuning this option can influence performance of reading.

    !!! warning

        Default value is different from Spark.

        Spark uses driver's own value, and it may be different in different drivers,
        and even versions of the same driver. For example, Oracle has
        default `fetchsize=10`, which is absolutely not usable.

        Thus we've overridden default value with `100_000`, which should increase reading performance.

    !!! info "Changed in 0.2.0"
        Set explicit default value to `100_000`
    """

    partitioning_mode: JDBCPartitioningMode = JDBCPartitioningMode.RANGE
    """Defines how Spark will parallelize reading from table.

    Possible values:

    * `range` (default)
        Allocate each executor a range of values from column passed into [partition_column][].

        ??? note "Spark generates for each executor an SQL query"

            Executor 1:

            ```sql
            SELECT ... FROM table
            WHERE (partition_column >= lowerBound
                    OR partition_column IS NULL)
            AND partition_column < (lowerBound + stride)
            ```
            Executor 2:

            ```sql
            SELECT ... FROM table
            WHERE partition_column >= (lowerBound + stride)
            AND partition_column < (lowerBound + 2 * stride)
            ```
            ...

            Executor N:

            ```sql
            SELECT ... FROM table
            WHERE partition_column >= (lowerBound + (N-1) * stride)
            AND partition_column <= upperBound
            ```
            Where `stride=(upperBound - lowerBound) / numPartitions`.

        Column type **must be** integer, date or timestamp.

        !!! note

            [lower_bound][], [upper_bound][] and [num_partitions][] are used just to
            calculate the partition stride, **NOT** for filtering the rows in table.
            So all rows in the table will be returned (unlike *Incremental* [strategy][]).

        !!! note

            All queries are executed in parallel. To execute them sequentially, use *Batch* [strategy][].

    * `hash`
        Allocate each executor a set of values based on hash of the [partition_column][] column.

        ??? note "Spark generates for each executor an SQL query"

            Executor 1:

            ```sql
            SELECT ... FROM table
            WHERE (some_hash(partition_column) mod num_partitions) = 0 -- lower_bound
            ```
            Executor 2:

            ```sql
            SELECT ... FROM table
            WHERE (some_hash(partition_column) mod num_partitions) = 1 -- lower_bound + 1
            ```
            ...

            Executor N:

            ```sql
            SELECT ... FROM table
            WHERE (some_hash(partition_column) mod num_partitions) = num_partitions-1 -- upper_bound
            ```
        !!! note

            The hash function implementation depends on RDBMS. It can be `MD5` or any other fast hash function,
            or expression based on this function call. Usually such functions accepts any column type as an input.

    * `mod`
        Allocate each executor a set of values based on modulus of the [partition_column][] column.

        ??? note "Spark generates for each executor an SQL query"

            Executor 1:

            ```sql
            SELECT ... FROM table
            WHERE (partition_column mod num_partitions) = 0 -- lower_bound
            ```
            Executor 2:

            ```sql
            SELECT ... FROM table
            WHERE (partition_column mod num_partitions) = 1 -- lower_bound + 1
            ```
            Executor N:

            ```sql
            SELECT ... FROM table
            WHERE (partition_column mod num_partitions) = num_partitions-1 -- upper_bound
            ```
        !!! note

            Can be used only with columns of integer type.

    !!! success "Added in 0.5.0"

    Examples
    --------

    Read data in 10 parallel jobs by range of values in `id_column` column:

    ```python
    ReadOptions(
        partitioning_mode="range",  # default mode, can be omitted
        partitionColumn="id_column",
        numPartitions=10,
        # Options below can be discarded because they are
        # calculated automatically as MIN and MAX values of `partitionColumn`
        lowerBound=0,
        upperBound=100_000,
    )
    ```
    Read data in 10 parallel jobs by hash of values in `some_column` column:

    ```python
    ReadOptions(
        partitioning_mode="hash",
        partitionColumn="some_column",
        numPartitions=10,
        # lowerBound and upperBound are automatically set to `0` and `9`
    )
    ```
    Read data in 10 parallel jobs by modulus of values in `id_column` column:

    ```python
    ReadOptions(
        partitioning_mode="mod",
        partitionColumn="id_column",
        numPartitions=10,
        # lowerBound and upperBound are automatically set to `0` and `9`
    )
    ```
    """

    @model_validator(mode="after")
    def _partitioning_mode_actions(self):
        if not self.partition_column:
            if self.num_partitions == 1:
                return self

            msg = "You should set partition_column to enable partitioning"
            raise ValueError(msg)

        if self.num_partitions == 1:
            msg = "You should set num_partitions > 1 to enable partitioning"
            raise ValueError(msg)

        if self.partitioning_mode == JDBCPartitioningMode.RANGE:
            return self

        if self.lower_bound is None:
            object.__setattr__(self, "lower_bound", 0)
        if self.upper_bound is None:
            object.__setattr__(self, "upper_bound", self.num_partitions)
        return self


class JDBCWriteOptions(GenericOptions):
    """Spark JDBC writing options.

    !!! success "Added in 0.5.0"
        Replace `SomeDB.Options` → `SomeDB.WriteOptions`

    Examples
    --------

    !!! note

        You can pass any value
        [supported by Spark](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html),
        even if it is not mentioned in this documentation. **Option names should be in** `camelCase`!

        The set of supported options depends on Spark version.

    ```python
    from onetl.connection import SomeDB

    options = SomeDB.WriteOptions(
        if_exists="append",
        batchsize=20_000,
        customSparkOption="value",
    )
    ```
    """

    model_config = ConfigDict(
        known_options=WRITE_OPTIONS | READ_WRITE_OPTIONS,  # type: ignore[typeddict-unknown-key]
        prohibited_options=GENERIC_PROHIBITED_OPTIONS | READ_OPTIONS,  # type: ignore[typeddict-unknown-key]
        extra="allow",
    )

    if_exists: JDBCTableExistBehavior = Field(  # type: ignore[literal-required]
        default=JDBCTableExistBehavior.APPEND,
        alias=avoid_alias("mode"),
    )
    """Behavior of writing data into existing table.

    Possible values:

    * `append` (default)
        Adds new rows into existing table.

        ??? note "Behavior in details"

            * Table does not exist
                Table is created using options provided by user
                (`createTableOptions`, `createTableColumnTypes`, etc).

            * Table exists
                Data is appended to a table. Table has the same DDL as before writing data

                !!! warning

                    This mode does not check whether table already contains
                    rows from dataframe, so duplicated rows can be created.

                    Also Spark does not support passing custom options to
                    insert statement, like `ON CONFLICT`, so don't try to
                    implement deduplication using unique indexes or constraints.

                    Instead, write to staging table and perform deduplication
                    using [execute][] method.

    * `replace_entire_table`
        **Table is dropped and then created, or truncated**.

        ??? note "Behavior in details"

            * Table does not exist
                Table is created using options provided by user
                (`createTableOptions`, `createTableColumnTypes`, etc).

            * Table exists
                Table content is replaced with dataframe content.

                After writing completed, target table could either have the same DDL as
                before writing data (`truncate=True`), or can be recreated (`truncate=False`
                or source does not support truncation).

    * `ignore`
        Ignores the write operation if the table already exists.

        ??? note "Behavior in details"

            * Table does not exist
                Table is created using options provided by user
                (`createTableOptions`, `createTableColumnTypes`, etc).

            * Table exists
                The write operation is ignored, and no data is written to the table.

    * `error`
        Raises an error if the table already exists.

        ??? note "Behavior in details"

            * Table does not exist
                Table is created using options provided by user
                (`createTableOptions`, `createTableColumnTypes`, etc).

            * Table exists
                An error is raised, and no data is written to the table.

    !!! info "Changed in 0.9.0"
        Renamed `mode` → `if_exists`
    """

    query_timeout: int | None = Field(default=None, alias="queryTimeout")
    """The number of seconds the driver will wait for a statement to execute.
    Zero means there is no limit.

    This option depends on driver implementation,
    some drivers can check the timeout of each query instead of an entire JDBC batch.
    """

    batchsize: int = 20_000
    """How many rows can be inserted per round trip.

    Tuning this option can influence performance of writing.

    !!! warning

        Default value is different from Spark.

        Spark uses quite small value `1000`, which is absolutely not usable
        in BigData world.

        Thus we've overridden default value with `20_000`,
        which should increase writing performance.

        You can increase it even more, up to `50_000`,
        but it depends on your database load and number of columns in the row.
        Higher values does not increase performance.

    !!! info "Changed in 0.4.0"
        Changed default value from 1000 to 20_000
    """

    isolation_level: str = Field(default="READ_UNCOMMITTED", alias="isolationLevel")
    """The transaction isolation level, which applies to current connection.

    Possible values:

    * `NONE` (as string, not Python's `None`)
    * `READ_COMMITTED`
    * `READ_UNCOMMITTED`
    * `REPEATABLE_READ`
    * `SERIALIZABLE`

    Values correspond to transaction isolation levels defined by JDBC standard.
    Please refer the documentation for
    [java.sql.Connection](https://docs.oracle.com/javase/8/docs/api/java/sql/Connection.html).
    """

    @model_validator(mode="before")
    @classmethod
    def _mode_is_deprecated(cls, values):
        if "mode" in values:
            warnings.warn(
                "Option `WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `WriteOptions(if_exists=...)` instead",
                category=UserWarning,
                stacklevel=3,
            )
        return values


class JDBCSQLOptions(GenericOptions):
    """Options specifically for SQL queries

    These options allow you to specify configurations for executing SQL queries
    without relying on Spark's partitioning mechanisms.

    !!! success "Added in 0.11.0"
        Split up `SomeDB.ReadOptions` to `SomeDB.SQLOptions`

    Examples
    --------

    !!! note

        You can pass any JDBC configuration
        [supported by Spark](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html),
        tailored to optimize SQL query execution. **Option names should be in** `camelCase`!

    ```python
    from onetl.connection import SomeDB

    options = SomeDB.SQLOptions(
        partitionColumn="reg_id",
        numPartitions=10,
        lowerBound=0,
        upperBound=1000,
        customSparkOption="value",
    )
    ```
    """

    partition_column: str | None = Field(default=None, alias="partitionColumn")
    """Column used to partition data across multiple executors for parallel query processing.

    !!! warning
        It is highly recommended to use primary key, or column with an index
        to avoid performance issues.

    ??? note "Example of using `partitionColumn="id"` with `partitioning_mode="range"`"

        ```sql
        -- If partition_column is 'id', with numPartitions=4, lowerBound=1, and upperBound=100:
        -- Executor 1 processes IDs from 1 to 25
        SELECT ... FROM table WHERE id >= 1 AND id < 26
        -- Executor 2 processes IDs from 26 to 50
        SELECT ... FROM table WHERE id >= 26 AND id < 51
        -- Executor 3 processes IDs from 51 to 75
        SELECT ... FROM table WHERE id >= 51 AND id < 76
        -- Executor 4 processes IDs from 76 to 100
        SELECT ... FROM table WHERE id >= 76 AND id <= 100


        -- General case for Executor N
        SELECT ... FROM table
        WHERE partition_column >= (lowerBound + (N-1) * stride)
        AND partition_column <= upperBound
        -- Where `stride` is calculated as `(upperBound - lowerBound) / numPartitions`.
        ```
    """

    num_partitions: int | None = Field(default=None, alias="numPartitions")
    """Number of jobs created by Spark to read the table content in parallel."""

    lower_bound: int | None = Field(default=None, alias="lowerBound")
    """Defines the lower boundary for partitioning the query's data. Mandatory if [partition_column][] is set"""

    upper_bound: int | None = Field(default=None, alias="upperBound")
    """Sets the lower boundary for data partitioning. Mandatory if [partition_column][] is set"""

    session_init_statement: str | None = Field(default=None, alias="sessionInitStatement")
    '''After each database session is opened to the remote DB and before starting to read data,
    this option executes a custom SQL statement (or a PL/SQL block).

    Use this to implement session initialization code.

    Example:

    ```python
    sessionInitStatement = """
        BEGIN
            execute immediate
            'alter session set "_serial_direct_read"=true';
        END;
    """
    ```
    '''

    query_timeout: int | None = Field(default=None, alias="queryTimeout")
    """The number of seconds the driver will wait for a statement to execute.
    Zero means there is no limit.

    This option depends on driver implementation,
    some drivers can check the timeout of each query instead of an entire JDBC batch.
    """

    fetchsize: int = 100_000
    """Fetch N rows from an opened cursor per one read round.

    Tuning this option can influence performance of reading.

    !!! warning

        Default value is different from Spark.

        Spark uses driver's own value, and it may be different in different drivers,
        and even versions of the same driver. For example, Oracle has
        default `fetchsize=10`, which is absolutely not usable.

        Thus we've overridden default value with `100_000`, which should increase reading performance.

    !!! info "Changed in 0.2.0"
        Set explicit default value to `100_000`
    """
    model_config = ConfigDict(
        known_options=READ_OPTIONS - {"partitioning_mode"},  # type: ignore[typeddict-unknown-key]
        prohibited_options=GENERIC_PROHIBITED_OPTIONS | WRITE_OPTIONS | {"partitioning_mode"},  # type: ignore[typeddict-unknown-key]
        extra="allow",
    )

    @model_validator(mode="after")
    def _check_partition_fields(self):
        if self.num_partitions is None:
            return self

        if self.num_partitions == 1:
            return self

        if self.lower_bound is not None and self.upper_bound is not None:
            return self

        msg = "lowerBound and upperBound must be set if numPartitions > 1"
        raise ValueError(msg)


@deprecated(
    "Deprecated in 0.5.0 and will be removed in 1.0.0. Use 'ReadOptions' or 'WriteOptions' instead",
    category=UserWarning,
)
class JDBCLegacyOptions(GenericOptions):
    model_config = ConfigDict(
        prohibited_options=GENERIC_PROHIBITED_OPTIONS,  # type: ignore[typeddict-unknown-key]
        known_options=READ_OPTIONS | WRITE_OPTIONS | READ_WRITE_OPTIONS,  # type: ignore[typeddict-unknown-key]
        extra="allow",
    )

    partition_column: str | None = Field(default=None, alias="partitionColumn")
    num_partitions: PositiveInt = Field(default=1, alias="numPartitions")
    lower_bound: int | None = Field(default=None, alias="lowerBound")
    upper_bound: int | None = Field(default=None, alias="upperBound")
    session_init_statement: str | None = Field(default=None, alias="sessionInitStatement")
    query_timeout: int | None = Field(default=None, alias="queryTimeout")
    if_exists: JDBCTableExistBehavior = Field(default=JDBCTableExistBehavior.APPEND, alias="mode")
    isolation_level: str = Field(default="READ_UNCOMMITTED", alias="isolationLevel")
    fetchsize: int = 100_000
    batchsize: int = 20_000
