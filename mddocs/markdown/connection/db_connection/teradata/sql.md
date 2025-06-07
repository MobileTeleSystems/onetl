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

### *pydantic model* onetl.connection.db_connection.teradata.options.TeradataSQLOptions

Options specifically for SQL queries

These options allow you to specify configurations for executing SQL queries
without relying on Spark’s partitioning mechanisms.

#### Versionadded
Added in version 0.11.0: Split up `Teradata.ReadOptions` to `Teradata.SQLOptions`

### Examples

#### NOTE
You can pass any JDBC configuration
[supported by Spark](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html),
tailored to optimize SQL query execution. **Option names should be in** `camelCase`!

```python
from onetl.connection import Teradata

options = Teradata.SQLOptions(
    partitionColumn="reg_id",
    numPartitions=10,
    lowerBound=0,
    upperBound=1000,
    customSparkOption="value",
)
```

<!-- !! processed by numpydoc !! -->

#### *field* partition_column *: str | None* *= None* *(alias 'partitionColumn')*

Column used to partition data across multiple executors for parallel query processing.

#### WARNING
It is highly recommended to use primary key, or column with an index
to avoid performance issues.

### Example of using `partitionColumn="id"` with `partitioning_mode="range"`

```sql
-- If partition_column is 'id', with numPartitions=4, lowerBound=1, and upperBound=100:
-- Executor 1 processes IDs from 1 to 25
SELECT ... FROM table WHERE id >= 1 AND id < 26
-- Executor 2 processes IDs from 26 to 50
SELECT ... FROM table WHERE id >= 26 AND id < 51
-- Executor 3 processes IDs from 51 to 75
SELECT ... FROM table WHERE id >= 51 AND id < 76
-- Executor 4 processes IDs from 76 to 100
SELECT ... FROM table WHERE id >= 76 AND id <= 100

-- General case for Executor N
SELECT ... FROM table
WHERE partition_column >= (lowerBound + (N-1) * stride)
AND partition_column <= upperBound
-- Where ``stride`` is calculated as ``(upperBound - lowerBound) / numPartitions``.
```

<!-- !! processed by numpydoc !! -->

#### *field* num_partitions *: int | None* *= None* *(alias 'numPartitions')*

Number of jobs created by Spark to read the table content in parallel.

<!-- !! processed by numpydoc !! -->

#### *field* lower_bound *: int | None* *= None* *(alias 'lowerBound')*

Defines the starting boundary for partitioning the query’s data. Mandatory if [`partition_column`](#onetl.connection.db_connection.teradata.options.TeradataSQLOptions.partition_column) is set

<!-- !! processed by numpydoc !! -->

#### *field* upper_bound *: int | None* *= None* *(alias 'upperBound')*

Sets the ending boundary for data partitioning. Mandatory if [`partition_column`](#onetl.connection.db_connection.teradata.options.TeradataSQLOptions.partition_column) is set

<!-- !! processed by numpydoc !! -->

#### *field* session_init_statement *: str | None* *= None* *(alias 'sessionInitStatement')*

After each database session is opened to the remote DB and before starting to read data,
this option executes a custom SQL statement (or a PL/SQL block).

Use this to implement session initialization code.

Example:

```python
sessionInitStatement = """
    BEGIN
        execute immediate
        'alter session set "_serial_direct_read"=true';
    END;
"""
```

<!-- !! processed by numpydoc !! -->

#### *field* query_timeout *: int | None* *= None* *(alias 'queryTimeout')*

The number of seconds the driver will wait for a statement to execute.
Zero means there is no limit.

This option depends on driver implementation,
some drivers can check the timeout of each query instead of an entire JDBC batch.

<!-- !! processed by numpydoc !! -->

#### *field* fetchsize *: int* *= 100000*

Fetch N rows from an opened cursor per one read round.

Tuning this option can influence performance of reading.

#### WARNING
Default value is different from Spark.

Spark uses driver’s own value, and it may be different in different drivers,
and even versions of the same driver. For example, Oracle has
default `fetchsize=10`, which is absolutely not usable.

Thus we’ve overridden default value with `100_000`, which should increase reading performance.

#### Versionchanged
Changed in version 0.2.0: Set explicit default value to `100_000`

<!-- !! processed by numpydoc !! -->
