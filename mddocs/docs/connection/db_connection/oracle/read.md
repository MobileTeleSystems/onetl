# Reading from Oracle using `DBReader` { #DBR-onetl-connection-db-connection-oracle-read-reading-from-oracle-using-dbreader }

[DBReader][DBR-onetl-db-reader] supports [strategy][DBR-onetl-strategy-read-strategies] for incremental data reading, but does not support custom queries, like `JOIN`.

!!! warning

    Please take into account [Oracle types][DBR-onetl-connection-db-connection-oracle-types-oracle-spark-type-mapping]

## Supported DBReader features { #DBR-onetl-connection-db-connection-oracle-read-supported-dbreader-features }

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, supported strategies:
    - ✅︎ [Snapshot strategy][DBR-onetl-strategy-snapshot-strategy]
    - ✅︎ [Incremental strategy][DBR-onetl-connection-db-connection-clickhouse-read-incremental-strategy]
    - ✅︎ [Snapshot batch strategy][DBR-onetl-strategy-snapshot-batch-strategy]
    - ✅︎ [Incremental batch strategy][DBR-onetl-strategy-incremental-batch-strategy]
- ✅︎ `hint` (see [official documentation](https://docs.oracle.com/cd/B10500_01/server.920/a96533/hintsref.htm))
- ❌ `df_schema`
- ✅︎ `options` (see [Oracle.ReadOptions][onetl.connection.db_connection.oracle.options.OracleReadOptions])

## Examples { #DBR-onetl-connection-db-connection-oracle-read-examples }

Snapshot strategy:

```python
from onetl.connection import Oracle
from onetl.db import DBReader

oracle = Oracle(...)

reader = DBReader(
    connection=oracle,
    source="schema.table",
    columns=["id", "key", "CAST(value AS VARCHAR2(4000)) value", "updated_dt"],
    where="key = 'something'",
    hint="INDEX(schema.table key_index)",
    options=Oracle.ReadOptions(partitionColumn="id", numPartitions=10),
)
df = reader.run()
```

Incremental strategy:

```python
from onetl.connection import Oracle
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

oracle = Oracle(...)

reader = DBReader(
    connection=oracle,
    source="schema.table",
    columns=["id", "key", "CAST(value AS VARCHAR2(4000)) value", "updated_dt"],
    where="key = 'something'",
    hint="INDEX(schema.table key_index)",
    hwm=DBReader.AutoDetectHWM(name="oracle_hwm", expression="updated_dt"),
    options=Oracle.ReadOptions(partitionColumn="id", numPartitions=10),
)

with IncrementalStrategy():
    df = reader.run()
```

## Recommendations { #DBR-onetl-connection-db-connection-oracle-read-recommendations }

### Select only required columns { #DBR-onetl-connection-db-connection-oracle-read-select-only-required-columns }

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names. This reduces the amount of data passed from Oracle to Spark.

### Pay attention to `where` value { #DBR-onetl-connection-db-connection-oracle-read-pay-attention-to-where-value }

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where="column = 'value'")` clause.
This both reduces the amount of data send from Oracle to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options { #DBR-onetl-connection-db-connection-oracle-read-options }


::: onetl.connection.db_connection.oracle.options.OracleReadOptions
    options:
        inherited_members: true
