<a id="mssql-read"></a>

# Reading from MSSQL using `DBReader`

`DBReader` supports [Read Strategies](../../../strategy/index.md#strategy) for incremental data reading,
but does not support custom queries, like `JOIN`.

#### WARNING
Please take into account [MSSQL <-> Spark type mapping](types.md#mssql-types)

## Supported DBReader features

* ✅︎ `columns`
* ✅︎ `where`
* ✅︎ `hwm`, supported strategies:
* * ✅︎ [Snapshot Strategy](../../../strategy/snapshot_strategy.md#snapshot-strategy)
* * ✅︎ [Incremental Strategy](../../../strategy/incremental_strategy.md#incremental-strategy)
* * ✅︎ [Snapshot Batch Strategy](../../../strategy/snapshot_batch_strategy.md#snapshot-batch-strategy)
* * ✅︎ [Incremental Batch Strategy](../../../strategy/incremental_batch_strategy.md#incremental-batch-strategy)
* ❌ `hint` (MSSQL does support hints, but DBReader not, at least for now)
* ❌ `df_schema`
* ✅︎ `options` (see `MSSQL.ReadOptions`)

## Examples

Snapshot strategy:

```python
from onetl.connection import MSSQL
from onetl.db import DBReader

mssql = MSSQL(...)

reader = DBReader(
    connection=mssql,
    source="schema.table",
    columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
    where="key = 'something'",
    options=MSSQL.ReadOptions(partitionColumn="id", numPartitions=10),
)
df = reader.run()
```

Incremental strategy:

```python
from onetl.connection import MSSQL
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

mssql = MSSQL(...)

reader = DBReader(
    connection=mssql,
    source="schema.table",
    columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
    where="key = 'something'",
    hwm=DBReader.AutoDetectHWM(name="mssql_hwm", expression="updated_dt"),
    options=MSSQL.ReadOptions(partitionColumn="id", numPartitions=10),
)

with IncrementalStrategy():
    df = reader.run()
```

## Recommendations

### Select only required columns

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names. This reduces the amount of data passed from MSSQL to Spark.

### Pay attention to `where` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where="column = 'value'")` clause.
This both reduces the amount of data send from MSSQL to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options
