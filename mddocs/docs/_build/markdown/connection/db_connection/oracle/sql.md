<a id="oracle-sql"></a>

# Reading from Oracle using `Oracle.sql`

`Oracle.sql` allows passing custom SQL query, but does not support incremental strategies.

#### WARNING
Please take into account [Oracle <-> Spark type mapping](types.md#oracle-types)

#### WARNING
Statement is executed in **read-write** connection, so if you’re calling some functions/procedures with DDL/DML statements inside,
they can change data in your database.

## Syntax support

Only queries with the following syntax are supported:

* ✅︎ `SELECT ... FROM ...`
* ✅︎ `WITH alias AS (...) SELECT ...`
* ❌ `SHOW ...`
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

## Examples

```python
from onetl.connection import Oracle

oracle = Oracle(...)
df = oracle.sql(
    """
    SELECT
        id,
        key,
        CAST(value AS VARCHAR2(4000)) value,
        updated_at
    FROM
        some.mytable
    WHERE
        key = 'something'
    """,
    options=Oracle.SQLOptions(
        partitionColumn="id",
        numPartitions=10,
        lowerBound=0,
        upperBound=1000,
    ),
)
```

## Recommendations

### Select only required columns

Instead of passing `SELECT * FROM ...` prefer passing exact column names `SELECT col1, col2, ...`.
This reduces the amount of data passed from Oracle to Spark.

### Pay attention to `where` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `WHERE column = 'value'` clause.
This both reduces the amount of data send from Oracle to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options
