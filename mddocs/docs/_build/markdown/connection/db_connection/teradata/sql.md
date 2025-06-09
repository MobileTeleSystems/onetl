<a id="teradata-sql"></a>

# Reading from Teradata using `Teradata.sql`

`Teradata.sql` allows passing custom SQL query, but does not support incremental strategies.

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
from onetl.connection import Teradata

teradata = Teradata(...)
df = teradata.sql(
    """
    SELECT
        id,
        key,
        CAST(value AS VARCHAR) AS value,
        updated_at,
        HASHAMP(HASHBUCKET(HASHROW(id))) MOD 10 AS part_column
    FROM
        database.mytable
    WHERE
        key = 'something'
    """,
    options=Teradata.SQLOptions(
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
This reduces the amount of data passed from Teradata to Spark.

### Pay attention to `where` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `WHERE column = 'value'` clause.
This both reduces the amount of data send from Teradata to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options
