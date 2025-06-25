# Writing to MySQL using `DBWriter` { #mysql-write }

For writing data to MySQL, use [DBWriter][db-writer].

!!! warning::

    Please take into account [MySQL types][mysql-types]

!!! warning

    It is always recommended to create table explicitly using [MySQL.execute][mysql-execute]
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected,
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

Method above accepts [MySQL.WriteOptions][onetl.connection.db_connection.mysql.options.MySQLWriteOptions]

<!-- 
```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.mysql.options
```

```{eval-rst}
.. autopydantic_model:: MySQLWriteOptions
    :inherited-members: GenericOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
```
 -->

::: onetl.connection.db_connection.mysql.options.MySQLWriteOptions
    options:
        members: true
        heading_level: 3
        show_root_heading: true