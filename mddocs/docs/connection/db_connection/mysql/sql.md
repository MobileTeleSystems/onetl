# Reading from MySQL using `MySQL.sql` { #DBR-onetl-connection-db-connection-mysql-sql-reading-from-mysql-using-mysql-sql }

`MySQL.sql` allows passing custom SQL query, but does not support incremental strategies.

!!! warning

    Please take into account [MySQL types][DBR-onetl-connection-db-connection-mysql-types-mysql-spark-type-mapping]

!!! warning

    Statement is executed in **read-write** connection, so if you're calling some functions/procedures with DDL/DML statements inside, they can change data in your database.

## Syntax support { #DBR-onetl-connection-db-connection-mysql-sql-syntax-support }

Only queries with the following syntax are supported:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ❌ `SHOW ...`
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

## Examples { #DBR-onetl-connection-db-connection-mysql-sql-examples }

```python
from onetl.connection import MySQL

mysql = MySQL(...)
df = mysql.sql(
    """
    SELECT
        id,
        key,
        CAST(value AS text) value,
        updated_at
    FROM
        some.mytable
    WHERE
        key = 'something'
    """,
    options=MySQL.SQLOptions(
        partitionColumn="id",
        numPartitions=10,
        lowerBound=0,
        upperBound=1000,
    ),
)
```

## Recommendations { #DBR-onetl-connection-db-connection-mysql-sql-recommendations }

### Select only required columns { #DBR-onetl-connection-db-connection-mysql-sql-select-only-required-columns }

Instead of passing `SELECT * FROM ...` prefer passing exact column names `SELECT col1, col2, ...`.
This reduces the amount of data passed from MySQL to Spark.

### Pay attention to `where` value { #DBR-onetl-connection-db-connection-mysql-sql-pay-attention-to-where-value }

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `WHERE column = 'value'` clause.
This both reduces the amount of data send from MySQL to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options { #DBR-onetl-connection-db-connection-mysql-sql-options }


::: onetl.connection.db_connection.mysql.options.MySQLSQLOptions
    options:
        inherited_members: true
        members: true
