# Writing to Oracle using `DBWriter` { #DBR-onetl-connection-db-connection-oracle-write-writing-to-oracle-using-dbwriter }

For writing data to Oracle, use [DBWriter][DBR-onetl-db-writer].

!!! warning

    Please take into account [Oracle types][DBR-onetl-connection-db-connection-oracle-types-oracle-spark-type-mapping]

!!! warning

    It is always recommended to create table explicitly using [Oracle.execute][DBR-onetl-connection-db-connection-oracle-execute-executing-statements-in-oracle] instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected, causing precision loss or other issues.

## Examples { #DBR-onetl-connection-db-connection-oracle-write-examples }

```python
from onetl.connection import Oracle
from onetl.db import DBWriter

oracle = Oracle(...)

df = ...  # data is here

writer = DBWriter(
    connection=oracle,
    target="schema.table",
    options=Oracle.WriteOptions(if_exists="append"),
)

writer.run(df)
```

## Options { #DBR-onetl-connection-db-connection-oracle-write-options }

Method above accepts [OracleWriteOptions][onetl.connection.db_connection.oracle.options.OracleWriteOptions]


::: onetl.connection.db_connection.oracle.options.OracleWriteOptions
    options:
        inherited_members: true
