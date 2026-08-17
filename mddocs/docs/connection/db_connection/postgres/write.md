# Writing to Postgres using `DBWriter` { #DBR-onetl-connection-db-connection-postgres-write-writing-to-postgres-using-dbwriter }

For writing data to Postgres, use [DBWriter][DBR-onetl-db-writer].

!!! warning

    Please take into account [Postgres types][DBR-onetl-connection-db-connection-postgres-types-postgres-spark-type-mapping]

!!! warning

    It is always recommended to create table explicitly using [Postgres.execute][DBR-onetl-connection-db-connection-postgres-execute-executing-statements-in-postgres]
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected,
    causing precision loss or other issues.

## Examples { #DBR-onetl-connection-db-connection-postgres-write-examples }

```python
from onetl.connection import Postgres
from onetl.db import DBWriter

postgres = Postgres(...)

df = ...  # data is here

writer = DBWriter(
    connection=postgres,
    target="schema.table",
    options=Postgres.WriteOptions(if_exists="append"),
)

writer.run(df)
```

## Options { #DBR-onetl-connection-db-connection-postgres-write-options }

Method above accepts [Postgres.WriteOptions][onetl.connection.db_connection.postgres.options.PostgresWriteOptions]


::: onetl.connection.db_connection.postgres.options.PostgresWriteOptions
    options:
        inherited_members: true
