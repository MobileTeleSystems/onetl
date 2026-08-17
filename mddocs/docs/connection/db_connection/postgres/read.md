# Reading from Postgres using `DBReader` { #DBR-onetl-connection-db-connection-postgres-read-reading-from-postgres-using-dbreader }

[DBReader][DBR-onetl-db-reader] supports [strategy][DBR-onetl-strategy-read-strategies] for incremental data reading, but does not support custom queries, like `JOIN`.

!!! warning

    Please take into account [Postgres types][DBR-onetl-connection-db-connection-postgres-types-postgres-spark-type-mapping]

## Supported DBReader features { #DBR-onetl-connection-db-connection-postgres-read-supported-dbreader-features }

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, supported strategies:
    - ✅︎ [Snapshot strategy][DBR-onetl-strategy-snapshot-strategy]
    - ✅︎ [Incremental strategy][DBR-onetl-connection-db-connection-clickhouse-read-incremental-strategy]
    - ✅︎ [Snapshot batch strategy][DBR-onetl-strategy-snapshot-batch-strategy]
    - ✅︎ [Incremental batch strategy][DBR-onetl-strategy-incremental-batch-strategy]
- ❌ `hint` (is not supported by Postgres)
- ❌ `df_schema`
- ✅︎ `options` (see [Postgres.ReadOptions][onetl.connection.db_connection.postgres.options.PostgresReadOptions])

## Examples { #DBR-onetl-connection-db-connection-postgres-read-examples }

Snapshot strategy:

```python
from onetl.connection import Postgres
from onetl.db import DBReader

postgres = Postgres(...)

reader = DBReader(
    connection=postgres,
    source="schema.table",
    columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
    where="key = 'something'",
    options=Postgres.ReadOptions(partitionColumn="id", numPartitions=10),
)
df = reader.run()
```

Incremental strategy:

```python
from onetl.connection import Postgres
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

postgres = Postgres(...)

reader = DBReader(
    connection=postgres,
    source="schema.table",
    columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
    where="key = 'something'",
    hwm=DBReader.AutoDetectHWM(name="postgres_hwm", expression="updated_dt"),
    options=Postgres.ReadOptions(partitionColumn="id", numPartitions=10),
)

with IncrementalStrategy():
    df = reader.run()
```

## Recommendations { #DBR-onetl-connection-db-connection-postgres-read-recommendations }

### Select only required columns { #DBR-onetl-connection-db-connection-postgres-read-select-only-required-columns }

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names. This reduces the amount of data passed from Postgres to Spark.

### Pay attention to `where` value { #DBR-onetl-connection-db-connection-postgres-read-pay-attention-to-where-value }

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where="column = 'value'")` clause.
This both reduces the amount of data send from Postgres to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options { #DBR-onetl-connection-db-connection-postgres-read-options }


::: onetl.connection.db_connection.postgres.options.PostgresReadOptions
    options:
        inherited_members: true
