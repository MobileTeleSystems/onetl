# Writing to Clickhouse using `DBWriter` { #DBR-onetl-connection-db-connection-clickhouse-write-writing-to-clickhouse-using-dbwriter }

For writing data to Clickhouse, use [DBWriter][DBR-onetl-db-writer].

!!! warning

    Please take into account [Clickhouse types][DBR-onetl-connection-db-connection-clickhouse-types-clickhouse-spark-type-mapping]


!!! warning

    It is always recommended to create table explicitly using [Clickhouse.execute][DBR-onetl-connection-db-connection-clickhouse-execute-executing-statements-in-clickhouse]
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected,
    causing precision loss or other issues.

## Examples { #DBR-onetl-connection-db-connection-clickhouse-write-examples }

```python
from onetl.connection import Clickhouse
from onetl.db import DBWriter

clickhouse = Clickhouse(...)

df = ...  # data is here

writer = DBWriter(
    connection=clickhouse,
    target="schema.table",
    options=Clickhouse.WriteOptions(
        if_exists="append",
        # ENGINE is required by Clickhouse
        createTableOptions="ENGINE = MergeTree() ORDER BY id",
    ),
)

writer.run(df)
```

## Options { #DBR-onetl-connection-db-connection-clickhouse-write-options }

Method above accepts [Clickhouse.WriteOptions][onetl.connection.db_connection.clickhouse.options.ClickhouseWriteOptions]


::: onetl.connection.db_connection.clickhouse.options.ClickhouseWriteOptions
    options:
        inherited_members: true
