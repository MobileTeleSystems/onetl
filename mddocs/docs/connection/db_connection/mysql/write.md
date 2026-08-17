# Writing to MySQL using `DBWriter` { #DBR-onetl-connection-db-connection-mysql-write-writing-to-mysql-using-dbwriter }

For writing data to MySQL, use [DBWriter][DBR-onetl-db-writer].

!!! warning

    Please take into account [MySQL types][DBR-onetl-connection-db-connection-mysql-types-mysql-spark-type-mapping]

!!! warning

    It is always recommended to create table explicitly using [MySQL.execute][DBR-onetl-connection-db-connection-mysql-execute-executing-statements-in-mysql] instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected, causing precision loss or other issues.

## Examples { #DBR-onetl-connection-db-connection-mysql-write-examples }

```python
from onetl.connection import MySQL
from onetl.db import DBWriter

mysql = MySQL(...)

df = ...  # data is here

writer = DBWriter(
    connection=mysql,
    target="schema.table",
    options=MySQL.WriteOptions(
        if_exists="append",
        # ENGINE is required by MySQL
        createTableOptions="ENGINE = MergeTree() ORDER BY id",
    ),
)

writer.run(df)
```

## Options { #DBR-onetl-connection-db-connection-mysql-write-options }

Method above accepts [MySQL.WriteOptions][onetl.connection.db_connection.mysql.options.MySQLWriteOptions]


::: onetl.connection.db_connection.mysql.options.MySQLWriteOptions
    options:
        inherited_members: true
        members: true
