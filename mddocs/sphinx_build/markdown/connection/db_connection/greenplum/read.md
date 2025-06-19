<a id="greenplum-read"></a>

# Reading from Greenplum using `DBReader`

Data can be read from Greenplum to Spark using `DBReader`.
It also supports [Read Strategies](../../../strategy/index.md#strategy) for incremental data reading.

#### WARNING
Please take into account [Greenplum <-> Spark type mapping](types.md#greenplum-types).

#### NOTE
Unlike JDBC connectors, *Greenplum connector for Spark* does not support
executing **custom** SQL queries using `.sql` method. Connector can be used to only read data from a table or view.

## Supported DBReader features

* ✅︎ `columns` (see note below)
* ✅︎ `where` (see note below)
* ✅︎ `hwm` (see note below), supported strategies:
* * ✅︎ [Snapshot Strategy](../../../strategy/snapshot_strategy.md#snapshot-strategy)
* * ✅︎ [Incremental Strategy](../../../strategy/incremental_strategy.md#incremental-strategy)
* * ✅︎ [Snapshot Batch Strategy](../../../strategy/snapshot_batch_strategy.md#snapshot-batch-strategy)
* * ✅︎ [Incremental Batch Strategy](../../../strategy/incremental_batch_strategy.md#incremental-batch-strategy)
* ❌ `hint` (is not supported by Greenplum)
* ❌ `df_schema`
* ✅︎ `options` (see `Greenplum.ReadOptions`)

#### WARNING
In case of Greenplum connector, `DBReader` does not generate raw `SELECT` query. Instead it relies on Spark SQL syntax
which in some cases (using column projection and predicate pushdown) can be converted to Greenplum SQL.

So `columns`, `where` and `hwm.expression` should be specified in [Spark SQL](https://spark.apache.org/docs/latest/sql-ref-syntax.html) syntax,
not Greenplum SQL.

This is OK:

```python
DBReader(
    columns=[
        "some_column",
        # this cast is executed on Spark side
        "CAST(another_column AS STRING)",
    ],
    # this predicate is parsed by Spark, and can be pushed down to Greenplum
    where="some_column LIKE 'val1%'",
)
```

This is will fail:

```python
DBReader(
    columns=[
        "some_column",
        # Spark does not have `text` type
        "CAST(another_column AS text)",
    ],
    # Spark does not support ~ syntax for regexp matching
    where="some_column ~ 'val1.*'",
)
```

## Examples

Snapshot strategy:

```python
from onetl.connection import Greenplum
from onetl.db import DBReader

greenplum = Greenplum(...)

reader = DBReader(
    connection=greenplum,
    source="schema.table",
    columns=["id", "key", "CAST(value AS string) value", "updated_dt"],
    where="key = 'something'",
)
df = reader.run()
```

Incremental strategy:

```python
from onetl.connection import Greenplum
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

greenplum = Greenplum(...)

reader = DBReader(
    connection=greenplum,
    source="schema.table",
    columns=["id", "key", "CAST(value AS string) value", "updated_dt"],
    where="key = 'something'",
    hwm=DBReader.AutoDetectHWM(name="greenplum_hwm", expression="updated_dt"),
)

with IncrementalStrategy():
    df = reader.run()
```

## Interaction schema

High-level schema is described in [Prerequisites](prerequisites.md#greenplum-prerequisites). You can find detailed interaction schema below.

### Spark <-> Greenplum interaction during DBReader.run()

## Recommendations

### Select only required columns

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names. This reduces the amount of data passed from Greenplum to Spark.

### Pay attention to `where` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where="column = 'value'")` clause.
This both reduces the amount of data send from Greenplum to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

### Read data in parallel

`DBReader` in case of Greenplum connector requires view or table to have a column which is used by Spark
for parallel reads.

Choosing proper column allows each Spark executor to read only part of data stored in the specified segment,
avoiding moving large amounts of data between segments, which improves reading performance.

#### Using `gp_segment_id`

By default, `DBReader` will use [gp_segment_id](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/troubleshooting.html#reading-from-a-view)
column for parallel data reading. Each DataFrame partition will contain data of a specific Greenplum segment.

This allows each Spark executor read only data from specific Greenplum segment, avoiding moving large amounts of data between segments.

If view is used, it is recommended to include `gp_segment_id` column to this view:

### Reading from view with gp_segment_id column

```python
from onetl.connection import Greenplum
from onetl.db import DBReader

greenplum = Greenplum(...)

greenplum.execute(
    """
    CREATE VIEW schema.view_with_gp_segment_id AS
    SELECT
        id,
        some_column,
        another_column,
        gp_segment_id  -- IMPORTANT
    FROM schema.some_table
    """,
)

reader = DBReader(
    connection=greenplum,
    source="schema.view_with_gp_segment_id",
)
df = reader.run()
```

#### Using custom `partition_column`

Sometimes table or view is lack of `gp_segment_id` column, but there is some column
with value range correlated with Greenplum segment distribution.

In this case, custom column can be used instead:

### Reading from view with custom partition_column

```python
from onetl.connection import Greenplum
from onetl.db import DBReader

greenplum = Greenplum(...)

greenplum.execute(
    """
    CREATE VIEW schema.view_with_partition_column AS
    SELECT
        id,
        some_column,
        part_column  -- correlated to greenplum segment ID
    FROM schema.some_table
    """,
)

reader = DBReader(
    connection=greenplum,
    source="schema.view_with_partition_column",
    options=Greenplum.ReadOptions(
        # parallelize data using specified column
        partitionColumn="part_column",
        # create 10 Spark tasks, each will read only part of table data
        partitions=10,
    ),
)
df = reader.run()
```

#### Reading `DISTRIBUTED REPLICATED` tables

Replicated tables do not have `gp_segment_id` column at all, so you need to set `partition_column` to some column name
of type integer/bigint/smallint.

### Parallel `JOIN` execution

In case of using views which require some data motion between Greenplum segments, like `JOIN` queries, another approach should be used.

Each Spark executor N will run the same query, so each of N query will start its own JOIN process, leading to really heavy load on Greenplum segments.
**This should be avoided**.

Instead is recommended to run `JOIN` query on Greenplum side, save the result to an intermediate table,
and then read this table using `DBReader`:

### Reading from view using intermediate table

```python
from onetl.connection import Greenplum
from onetl.db import DBReader

greenplum = Greenplum(...)

greenplum.execute(
    """
    CREATE UNLOGGED TABLE schema.intermediate_table AS
    SELECT
        id,
        tbl1.col1,
        tbl1.data,
        tbl2.another_data
    FROM
        schema.table1 as tbl1
    JOIN
        schema.table2 as tbl2
    ON
        tbl1.col1 = tbl2.col2
    WHERE ...
    """,
)

reader = DBReader(
    connection=greenplum,
    source="schema.intermediate_table",
)
df = reader.run()

# write dataframe somethere

greenplum.execute(
    """
    DROP TABLE schema.intermediate_table
    """,
)
```

#### WARNING
**NEVER** do that:

```python
df1 = DBReader(connection=greenplum, target="public.table1", ...).run()
df2 = DBReader(connection=greenplum, target="public.table2", ...).run()

joined_df = df1.join(df2, on="col")
```

This will lead to sending all the data from both `table1` and `table2` to Spark executor memory, and then `JOIN`
will be performed on Spark side, not inside Greenplum. This is **VERY** inefficient.

#### `TEMPORARY` tables notice

Someone could think that writing data from view or result of `JOIN` to `TEMPORARY` table,
and then passing it to `DBReader`, is an efficient way to read data from Greenplum. This is because temp tables are not generating WAL files,
and are automatically deleted after finishing the transaction.

That will **NOT** work. Each Spark executor establishes its own connection to Greenplum.
And each connection starts its own transaction which means that every executor will read empty temporary table.

You should use [UNLOGGED](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-sql_commands-CREATE_TABLE.html) tables
to write data to intermediate table without generating WAL logs.

## Options
