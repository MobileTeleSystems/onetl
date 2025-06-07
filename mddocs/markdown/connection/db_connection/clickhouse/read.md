<a id="clickhouse-read"></a>

# Reading from Clickhouse using `DBReader`

[`DBReader`](../../../db/db_reader.md#onetl.db.db_reader.db_reader.DBReader) supports [Read Strategies](../../../strategy/index.md#strategy) for incremental data reading,
but does not support custom queries, like `JOIN`.

#### WARNING
Please take into account [Clickhouse <-> Spark type mapping](types.md#clickhouse-types)

## Supported DBReader features

* ✅︎ `columns`
* ✅︎ `where`
* ✅︎ `hwm`, supported strategies:
* * ✅︎ [Snapshot Strategy](../../../strategy/snapshot_strategy.md#snapshot-strategy)
* * ✅︎ [Incremental Strategy](../../../strategy/incremental_strategy.md#incremental-strategy)
* * ✅︎ [Snapshot Batch Strategy](../../../strategy/snapshot_batch_strategy.md#snapshot-batch-strategy)
* * ✅︎ [Incremental Batch Strategy](../../../strategy/incremental_batch_strategy.md#incremental-batch-strategy)
* ❌ `hint` (is not supported by Clickhouse)
* ❌ `df_schema`
* ✅︎ `options` (see [`Clickhouse.ReadOptions`](#onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions))

## Examples

Snapshot strategy:

```python
from onetl.connection import Clickhouse
from onetl.db import DBReader

clickhouse = Clickhouse(...)

reader = DBReader(
    connection=clickhouse,
    source="schema.table",
    columns=["id", "key", "CAST(value AS String) value", "updated_dt"],
    where="key = 'something'",
    options=Clickhouse.ReadOptions(partitionColumn="id", numPartitions=10),
)
df = reader.run()
```

Incremental strategy:

```python
from onetl.connection import Clickhouse
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

clickhouse = Clickhouse(...)

reader = DBReader(
    connection=clickhouse,
    source="schema.table",
    columns=["id", "key", "CAST(value AS String) value", "updated_dt"],
    where="key = 'something'",
    hwm=DBReader.AutoDetectHWM(name="clickhouse_hwm", expression="updated_dt"),
    options=Clickhouse.ReadOptions(partitionColumn="id", numPartitions=10),
)

with IncrementalStrategy():
    df = reader.run()
```

## Recommendations

### Select only required columns

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names. This reduces the amount of data passed from Clickhouse to Spark.

### Pay attention to `where` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where="column = 'value'")` clause.
This both reduces the amount of data send from Clickhouse to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options

### *pydantic model* onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions

Spark JDBC reading options.

#### Versionadded
Added in version 0.5.0: Replace `Clickhouse.Options` → `Clickhouse.ReadOptions`

### Examples

#### NOTE
You can pass any value
[supported by Spark](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html),
even if it is not mentioned in this documentation. **Option names should be in** `camelCase`!

The set of supported options depends on Spark version.

```python
from onetl.connection import Clickhouse

options = Clickhouse.ReadOptions(
    partitioning_mode="range",
    partitionColumn="reg_id",
    numPartitions=10,
    customSparkOption="value",
)
```

<!-- !! processed by numpydoc !! -->

#### *field* partition_column *: str | None* *= None* *(alias 'partitionColumn')*

Column used to parallelize reading from a table.

#### WARNING
It is highly recommended to use primary key, or column with an index
to avoid performance issues.

#### NOTE
Column type depends on [`partitioning_mode`](#onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions.partitioning_mode).

* `partitioning_mode="range"` requires column to be an integer, date or timestamp (can be NULL, but not recommended).
* `partitioning_mode="hash"` accepts any column type (NOT NULL).
* `partitioning_mode="mod"` requires column to be an integer (NOT NULL).

See documentation for [`partitioning_mode`](#onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions.partitioning_mode) for more details

<!-- !! processed by numpydoc !! -->

#### *field* num_partitions *: PositiveInt* *= 1* *(alias 'numPartitions')*

Number of jobs created by Spark to read the table content in parallel.
See documentation for [`partitioning_mode`](#onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions.partitioning_mode) for more details

<!-- !! processed by numpydoc !! -->

#### *field* lower_bound *: int | None* *= None* *(alias 'lowerBound')*

See documentation for [`partitioning_mode`](#onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions.partitioning_mode) for more details

<!-- !! processed by numpydoc !! -->

#### *field* upper_bound *: int | None* *= None* *(alias 'upperBound')*

See documentation for [`partitioning_mode`](#onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions.partitioning_mode) for more details

<!-- !! processed by numpydoc !! -->

#### *field* session_init_statement *: str | None* *= None* *(alias 'sessionInitStatement')*

After each database session is opened to the remote DB and before starting to read data,
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

<!-- !! processed by numpydoc !! -->

#### *field* query_timeout *: int | None* *= None* *(alias 'queryTimeout')*

The number of seconds the driver will wait for a statement to execute.
Zero means there is no limit.

This option depends on driver implementation,
some drivers can check the timeout of each query instead of an entire JDBC batch.

<!-- !! processed by numpydoc !! -->

#### *field* fetchsize *: int* *= 100000*

Fetch N rows from an opened cursor per one read round.

Tuning this option can influence performance of reading.

#### WARNING
Default value is different from Spark.

Spark uses driver’s own value, and it may be different in different drivers,
and even versions of the same driver. For example, Oracle has
default `fetchsize=10`, which is absolutely not usable.

Thus we’ve overridden default value with `100_000`, which should increase reading performance.

#### Versionchanged
Changed in version 0.2.0: Set explicit default value to `100_000`

<!-- !! processed by numpydoc !! -->

#### *field* partitioning_mode *: JDBCPartitioningMode* *= JDBCPartitioningMode.RANGE*

Defines how Spark will parallelize reading from table.

Possible values:

* `range` (default)
  : Allocate each executor a range of values from column passed into [`partition_column`](#onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions.partition_column).
    <br/>
    ### Spark generates for each executor an SQL query
    <br/>
    Executor 1:
    ```sql
    SELECT ... FROM table
    WHERE (partition_column >= lowerBound
            OR partition_column IS NULL)
    AND partition_column < (lower_bound + stride)
    ```
    <br/>
    Executor 2:
    ```sql
    SELECT ... FROM table
    WHERE partition_column >= (lower_bound + stride)
    AND partition_column < (lower_bound + 2 * stride)
    ```
    <br/>
    …
    <br/>
    Executor N:
    ```sql
    SELECT ... FROM table
    WHERE partition_column >= (lower_bound + (N-1) * stride)
    AND partition_column <= upper_bound
    ```
    <br/>
    Where `stride=(upper_bound - lower_bound) / num_partitions`.
    <br/>
    #### NOTE
    Can be used only with columns of integer, date or timestamp types.
    <br/>
    #### NOTE
    [`lower_bound`](#onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions.lower_bound), [`upper_bound`](#onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions.upper_bound) and [`num_partitions`](#onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions.num_partitions) are used just to
    calculate the partition stride, **NOT** for filtering the rows in table.
    So all rows in the table will be returned (unlike *Incremental* [Read Strategies](../../../strategy/index.md#strategy)).
    <br/>
    #### NOTE
    All queries are executed in parallel. To execute them sequentially, use *Batch* [Read Strategies](../../../strategy/index.md#strategy).
* `hash`
  : Allocate each executor a set of values based on hash of the [`partition_column`](#onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions.partition_column) column.
    <br/>
    ### Spark generates for each executor an SQL query
    <br/>
    Executor 1:
    ```sql
    SELECT ... FROM table
    WHERE (some_hash(partition_column) mod num_partitions) = 0 -- lower_bound
    ```
    <br/>
    Executor 2:
    ```sql
    SELECT ... FROM table
    WHERE (some_hash(partition_column) mod num_partitions) = 1 -- lower_bound + 1
    ```
    <br/>
    …
    <br/>
    Executor N:
    ```sql
    SELECT ... FROM table
    WHERE (some_hash(partition_column) mod num_partitions) = num_partitions-1 -- upper_bound
    ```
    <br/>
    #### NOTE
    The hash function implementation depends on RDBMS. It can be `MD5` or any other fast hash function,
    or expression based on this function call. Usually such functions accepts any column type as an input.
* `mod`
  : Allocate each executor a set of values based on modulus of the [`partition_column`](#onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions.partition_column) column.
    <br/>
    ### Spark generates for each executor an SQL query
    <br/>
    Executor 1:
    ```sql
    SELECT ... FROM table
    WHERE (partition_column mod num_partitions) = 0 -- lower_bound
    ```
    <br/>
    Executor 2:
    ```sql
    SELECT ... FROM table
    WHERE (partition_column mod num_partitions) = 1 -- lower_bound + 1
    ```
    <br/>
    Executor N:
    ```sql
    SELECT ... FROM table
    WHERE (partition_column mod num_partitions) = num_partitions-1 -- upper_bound
    ```
    <br/>
    #### NOTE
    Can be used only with columns of integer type.

#### Versionadded
Added in version 0.5.0.

### Examples

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

<!-- !! processed by numpydoc !! -->
