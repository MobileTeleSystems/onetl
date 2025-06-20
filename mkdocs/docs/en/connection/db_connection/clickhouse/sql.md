(clickhouse-sql)=

# Reading from Clickhouse using `Clickhouse.sql`

`Clickhouse.sql` allows passing custom SQL query, but does not support incremental strategies.

```{eval-rst}
.. warning::

    Please take into account :ref:`clickhouse-types`
```

```{eval-rst}
.. warning::

    Statement is executed in **read-write** connection, so if you're calling some functions/procedures with DDL/DML statements inside,
    they can change data in your database.
```

## Syntax support

Only queries with the following syntax are supported:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

## Examples

```python
from onetl.connection import Clickhouse

clickhouse = Clickhouse(...)
df = clickhouse.sql(
    """
    SELECT
        id,
        key,
        CAST(value AS String) value,
        updated_at
    FROM
        some.mytable
    WHERE
        key = 'something'
    """,
    options=Clickhouse.SQLOptions(
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
This reduces the amount of data passed from Clickhouse to Spark.

### Pay attention to `where` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `WHERE column = 'value'` clause.
This both reduces the amount of data send from Clickhouse to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options

```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.clickhouse.options
```

```{eval-rst}
.. autopydantic_model:: ClickhouseSQLOptions
    :inherited-members: GenericOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
```
