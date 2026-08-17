# Reading from Clickhouse using `DBReader` { #DBR-onetl-connection-db-connection-clickhouse-read-reading-from-clickhouse-using-dbreader }

[DBReader][DBR-onetl-db-reader] supports [strategy][DBR-onetl-strategy-read-strategies] for incremental data reading,
but does not support custom queries, like `JOIN`.

!!! warning

    Please take into account [Сlickhouse types][DBR-onetl-connection-db-connection-clickhouse-types-clickhouse-spark-type-mapping]

## Supported DBReader features { #DBR-onetl-connection-db-connection-clickhouse-read-supported-dbreader-features }

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, supported strategies:
    - ✅︎ [Snapshot strategy][DBR-onetl-strategy-snapshot-strategy]
    - ✅︎ [Incremental strategy][DBR-onetl-connection-db-connection-clickhouse-read-incremental-strategy]
    - ✅︎ [Snapshot batch strategy][DBR-onetl-strategy-snapshot-batch-strategy]
    - ✅︎ [Incremental batch strategy][DBR-onetl-strategy-incremental-batch-strategy]
- ❌ `hint` (is not supported by Clickhouse)
- ❌ `df_schema`
- ✅︎ `options` (see [Clickhouse.ReadOptions][onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions])

## Examples { #DBR-onetl-connection-db-connection-clickhouse-read-examples }

### Snapshot strategy { #DBR-onetl-connection-db-connection-clickhouse-read-snapshot-strategy }

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

### Incremental strategy { #DBR-onetl-connection-db-connection-clickhouse-read-incremental-strategy }

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

## Recommendations { #DBR-onetl-connection-db-connection-clickhouse-read-recommendations }

### Select only required columns { #DBR-onetl-connection-db-connection-clickhouse-read-select-only-required-columns }

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names. This reduces the amount of data passed from Clickhouse to Spark.

### Pay attention to `where` value { #DBR-onetl-connection-db-connection-clickhouse-read-pay-attention-to-where-value }

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where="column = 'value'")` clause.
This both reduces the amount of data send from Clickhouse to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options { #DBR-onetl-connection-db-connection-clickhouse-read-options }


::: onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions
    options:
        inherited_members: true
