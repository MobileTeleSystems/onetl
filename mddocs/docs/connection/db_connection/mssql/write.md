# Writing to MSSQL using `DBWriter` { #DBR-onetl-connection-db-connection-mssql-write-writing-to-mssql-using-dbwriter }

For writing data to MSSQL, use [DBWriter][DBR-onetl-db-writer].

!!! warning

    Please take into account [MSSQL types][DBR-onetl-connection-db-connection-mssql-types-mssql-spark-type-mapping]

!!! warning

    It is always recommended to create table explicitly using [MSSQL.execute][DBR-onetl-connection-db-connection-mssql-execute-executing-statements-in-mssql]
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected,
    causing precision loss or other issues.

## Examples { #DBR-onetl-connection-db-connection-mssql-write-examples }

```python
from onetl.connection import MSSQL
from onetl.db import DBWriter

mssql = MSSQL(...)

df = ...  # data is here

writer = DBWriter(
    connection=mssql,
    target="schema.table",
    options=MSSQL.WriteOptions(if_exists="append"),
)

writer.run(df)
```

## Options { #DBR-onetl-connection-db-connection-mssql-write-options }

Method above accepts [MSSQL.WriteOptions][onetl.connection.db_connection.mssql.options.MSSQLWriteOptions]


::: onetl.connection.db_connection.mssql.options.MSSQLWriteOptions
    options:
        inherited_members: true
