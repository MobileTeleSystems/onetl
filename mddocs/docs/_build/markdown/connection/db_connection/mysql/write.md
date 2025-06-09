<a id="mysql-write"></a>

# Writing to MySQL using `DBWriter`

For writing data to MySQL, use `DBWriter`.

#### WARNING
Please take into account [MySQL <-> Spark type mapping](types.md#mysql-types)

#### WARNING
It is always recommended to create table explicitly using [MySQL.execute](execute.md#mysql-execute)
instead of relying on Spark’s table DDL generation.

This is because Spark’s DDL generator can create columns with different precision and types than it is expected,
causing precision loss or other issues.

## Examples

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

## Options

Method above accepts  `MySQL.WriteOptions`
