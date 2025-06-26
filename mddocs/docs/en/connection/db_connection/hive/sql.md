# Reading from Hive using `Hive.sql` { #hive-sql }

`Hive.sql` allows passing custom SQL query, but does not support incremental strategies.

## Syntax support

Only queries with the following syntax are supported:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

!!! warning

    Actually, query should be written using [SparkSQL](https://spark.apache.org/docs/latest/sql-ref-syntax.html#data-retrieval-statements) syntax, not HiveQL.

## Examples

```python
from onetl.connection import Hive

hive = Hive(...)
df = hive.sql(
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
    """
)
```

## Recommendations

### Use column-based write formats

Prefer these write formats:
: - [ORC](https://spark.apache.org/docs/latest/sql-data-sources-orc.html)
  - [Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
  - [Iceberg](https://iceberg.apache.org/spark-quickstart/)
  - [Hudi](https://hudi.apache.org/docs/quick-start-guide/)
  - [Delta](https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake)

For colum-based write formats, each file contains separated sections there column data is stored. The file footer contains
location of each column section/group. Spark can use this information to load only sections required by specific query, e.g. only selected columns,
to drastically speed up the query.

Another advantage is high compression ratio, e.g. 10x-100x in comparison to JSON or CSV.

### Select only required columns

Instead of passing `SELECT * FROM ...` prefer passing exact column names `SELECT col1, col2, ...`.
This drastically reduces the amount of data read by Spark, **if column-based file formats are used**.

### Use partition columns in `where` clause

Queries should include `WHERE` clause with filters on Hive partitioning columns.
This allows Spark to read only small set of files (*partition pruning*) instead of scanning the entire table, so this drastically increases performance.

Supported operators are: `=`, `>`, `<` and `BETWEEN`, and only against some **static** value.

## Details


<!-- 
```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.hive.connection
```

```{eval-rst}
.. automethod:: Hive.sql
```
 -->

::: onetl.connection.db_connection.hive.connection.Hive.sql
    options:
        members:
            - sql
