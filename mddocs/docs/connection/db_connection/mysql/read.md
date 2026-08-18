# Reading from MySQL using `DBReader` { #DBR-onetl-connection-db-connection-mysql-read-reading-from-mysql-using-dbreader }

[DBReader][DBR-onetl-db-reader] supports [strategy][DBR-onetl-strategy-read-strategies] for incremental data reading, but does not support custom queries, like `JOIN`.

!!! warning

    Please take into account [MySQL types][DBR-onetl-connection-db-connection-mysql-types-mysql-spark-type-mapping]

## Supported DBReader features { #DBR-onetl-connection-db-connection-mysql-read-supported-dbreader-features }

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, supported strategies:
    - ✅︎ [Snapshot strategy][DBR-onetl-strategy-snapshot-strategy]
    - ✅︎ [Incremental strategy][DBR-onetl-connection-db-connection-clickhouse-read-incremental-strategy]
    - ✅︎ [Snapshot batch strategy][DBR-onetl-strategy-snapshot-batch-strategy]
    - ✅︎ [Incremental batch strategy][DBR-onetl-strategy-incremental-batch-strategy]
- ✅︎ `hint` (see [official documentation](https://dev.mysql.com/doc/refman/en/optimizer-hints.html))
- ❌ `df_schema`
- ✅︎ `options` (see [MySQL.ReadOptions][onetl.connection.db_connection.mysql.options.MySQLReadOptions])

## Examples { #DBR-onetl-connection-db-connection-mysql-read-examples }

Snapshot strategy:

```python
from onetl.connection import MySQL
from onetl.db import DBReader

mysql = MySQL(...)

reader = DBReader(
    connection=mysql,
    source="schema.table",
    columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
    where="key = 'something'",
    hint="SKIP_SCAN(schema.table key_index)",
    options=MySQL.ReadOptions(partitionColumn="id", numPartitions=10),
)
df = reader.run()
```

Incremental strategy:

```python
from onetl.connection import MySQL
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

mysql = MySQL(...)

reader = DBReader(
    connection=mysql,
    source="schema.table",
    columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
    where="key = 'something'",
    hint="SKIP_SCAN(schema.table key_index)",
    hwm=DBReader.AutoDetectHWM(name="mysql_hwm", expression="updated_dt"),
    options=MySQL.ReadOptions(partitionColumn="id", numPartitions=10),
)

with IncrementalStrategy():
    df = reader.run()
```

## Recommendations { #DBR-onetl-connection-db-connection-mysql-read-recommendations }

### Select only required columns { #DBR-onetl-connection-db-connection-mysql-read-select-only-required-columns }

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names. This reduces the amount of data passed from MySQL to Spark.

### Pay attention to `where` value { #DBR-onetl-connection-db-connection-mysql-read-pay-attention-to-where-value }

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where="column = 'value'")` clause.
This both reduces the amount of data send from MySQL to Spark, and may also improve performance of the query.
Especially if there are indexes for columns used in `where` clause.

## Options { #DBR-onetl-connection-db-connection-mysql-read-options }


::: onetl.connection.db_connection.mysql.options.MySQLReadOptions
    options:
        inherited_members: true
        members: true
