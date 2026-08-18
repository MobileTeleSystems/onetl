# Reading from MSSQL using `DBReader` { #DBR-onetl-connection-db-connection-mssql-read-reading-from-mssql-using-dbreader }

[DBReader][DBR-onetl-db-reader] supports [strategy][DBR-onetl-strategy-read-strategies] for incremental data reading, but does not support custom queries, like `JOIN`.

!!! warning

    Please take into account [MSSQL types][DBR-onetl-connection-db-connection-mssql-types-mssql-spark-type-mapping]

## Supported DBReader features { #DBR-onetl-connection-db-connection-mssql-read-supported-dbreader-features }

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, supported strategies:
    - ✅︎ [Snapshot strategy][DBR-onetl-strategy-snapshot-strategy]
    - ✅︎ [Incremental strategy][DBR-onetl-connection-db-connection-clickhouse-read-incremental-strategy]
    - ✅︎ [Snapshot batch strategy][DBR-onetl-strategy-snapshot-batch-strategy]
    - ✅︎ [Incremental batch strategy][DBR-onetl-strategy-incremental-batch-strategy]
- ❌ `hint` (MSSQL does support hints, but DBReader not, at least for now)
- ❌ `df_schema`
- ✅︎ `options` (see [MSSQL.ReadOptions][onetl.connection.db_connection.mssql.options.MSSQLReadOptions])

## Examples { #DBR-onetl-connection-db-connection-mssql-read-examples }

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

## Recommendations { #DBR-onetl-connection-db-connection-mssql-read-recommendations }

### Select only required columns { #DBR-onetl-connection-db-connection-mssql-read-select-only-required-columns }

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names. This reduces the amount of data passed from MSSQL to Spark.

### Pay attention to `where` value { #DBR-onetl-connection-db-connection-mssql-read-pay-attention-to-where-value }

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where="column = 'value'")` clause.
This both reduces the amount of data send from MSSQL to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options { #DBR-onetl-connection-db-connection-mssql-read-options }


::: onetl.connection.db_connection.mssql.options.MSSQLReadOptions
    options:
        inherited_members: true
