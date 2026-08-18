# Reading from MSSQL using `MSSQL.sql` { #DBR-onetl-connection-db-connection-mssql-sql-reading-from-mssql-using-mssql-sql }

`MSSQL.sql` allows passing custom SQL query, but does not support incremental strategies.

!!! warning

    Please take into account [MSSQL types][DBR-onetl-connection-db-connection-mssql-types-mssql-spark-type-mapping]

!!! warning

    Statement is executed in **read-write** connection, so if you're calling some functions/procedures with DDL/DML statements inside,
    they can change data in your database.

## Syntax support { #DBR-onetl-connection-db-connection-mssql-sql-syntax-support }

Only queries with the following syntax are supported:

- ✅︎ `SELECT ... FROM ...`
- ❌ `WITH alias AS (...) SELECT ...`
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

## Examples { #DBR-onetl-connection-db-connection-mssql-sql-examples }

```python
from onetl.connection import MSSQL

mssql = MSSQL(...)
df = mssql.sql(
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
    options=MSSQL.SQLOptions(
        partitionColumn="id",
        numPartitions=10,
        lowerBound=0,
        upperBound=1000,
    ),
)
```

## Recommendations { #DBR-onetl-connection-db-connection-mssql-sql-recommendations }

### Select only required columns { #DBR-onetl-connection-db-connection-mssql-sql-select-only-required-columns }

Instead of passing `SELECT * FROM ...` prefer passing exact column names `SELECT col1, col2, ...`.
This reduces the amount of data passed from MSSQL to Spark.

### Pay attention to `where` value { #DBR-onetl-connection-db-connection-mssql-sql-pay-attention-to-where-value }

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `WHERE column = 'value'` clause.
This both reduces the amount of data send from MSSQL to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options { #DBR-onetl-connection-db-connection-mssql-sql-options }


::: onetl.connection.db_connection.mssql.options.MSSQLSQLOptions
    options:
        inherited_members: true
