# MSSQL \<-> Spark type mapping { #mssql-types }

!!! note

    The results below are valid for Spark 3.5.5, and may differ on other Spark versions.

## Type detection & casting

Spark's DataFrames always have a `schema` which is a list of columns with corresponding Spark types. All operations on a column are performed using column type.

### Reading from MSSQL

This is how MSSQL connector performs this:

- For each column in query result (`SELECT column1, column2, ... FROM table ...`) get column name and MSSQL type.
- Find corresponding `MSSQL type (read)` → `Spark type` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- Create DataFrame from query with specific column names and Spark types.

### Writing to some existing MSSQL table

This is how MSSQL connector performs this:

- Get names of columns in DataFrame. [^1]
- Perform `SELECT * FROM table LIMIT 0` query.
- Take only columns present in DataFrame (by name, case insensitive). For each found column get MSSQL type.
- Find corresponding `Spark type` → `MSSQL type (write)` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- If `MSSQL type (write)` match `MSSQL type (read)`, no additional casts will be performed, DataFrame column will be written to MSSQL as is.
- If `MSSQL type (write)` does not match `MSSQL type (read)`, DataFrame column will be casted to target column type **on MSSQL side**.
  For example, you can write column with text data to `int` column, if column contains valid integer values within supported value range and precision [^2].

[^1]: This allows to write data to tables with `DEFAULT` and `GENERATED` columns - if DataFrame has no such column,
    it will be populated by MSSQL.

[^2]: This is true only if DataFrame column is a `StringType()`, because text value is parsed automatically to target column type.

    But other types cannot be silently converted, like `int -> text`. This requires explicit casting, see [DBWriter].

### Create new table using Spark

!!! warning

    ABSOLUTELY NOT RECOMMENDED!

This is how MSSQL connector performs this:

- Find corresponding `Spark type` → `MSSQL type (create)` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- Generate DDL for creating table in MSSQL, like `CREATE TABLE (col1 ...)`, and run it.
- Write DataFrame to created table as is.

But some cases this may lead to using wrong column type. For example, Spark creates column of type `timestamp`
which corresponds to MSSQL's type `timestamp(0)` (precision up to seconds)
instead of more precise `timestamp(6)` (precision up to nanoseconds).
This may lead to incidental precision loss, or sometimes data cannot be written to created table at all.

So instead of relying on Spark to create tables:

??? note "See example"

    ```python

        writer = DBWriter(
            connection=mssql,
            target="myschema.target_tbl",
            options=MSSQL.WriteOptions(
                if_exists="append",
            ),
        )
        writer.run(df)
    ```

Always prefer creating tables with specific types **BEFORE WRITING DATA**:

??? note "See example"

    ```python

        mssql.execute(
            """
            CREATE TABLE schema.table (
                id bigint,
                key text,
                value datetime2(6) -- specific type and precision
            )
            """,
        )

        writer = DBWriter(
            connection=mssql,
            target="myschema.target_tbl",
            options=MSSQL.WriteOptions(if_exists="append"),
        )
        writer.run(df)
    ```

### References

Here you can find source code with type conversions:

- [MSSQL -> JDBC](https://github.com/microsoft/mssql-jdbc/blob/v12.2.0/src/main/java/com/microsoft/sqlserver/jdbc/SQLServerResultSetMetaData.java#L117-L170)
- [JDBC -> Spark](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/MsSqlServerDialect.scala#L135-L152)
- [Spark -> JDBC](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/MsSqlServerDialect.scala#L154-L163)
- [JDBC -> MSSQL](https://github.com/microsoft/mssql-jdbc/blob/v12.2.0/src/main/java/com/microsoft/sqlserver/jdbc/DataTypes.java#L625-L676)

## Supported types

See [official documentation](https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql)

### Numeric types

| MSSQL type (read)             | Spark type                        | MSSQL type (write)            | MSSQL type (create)           |
|-------------------------------|-----------------------------------|-------------------------------|-------------------------------|
| `decimal`                   | `DecimalType(P=18, S=0)`        | `decimal(P=18, S=0)`        | `decimal(P=18, S=0)`        |
| `decimal(P=0..38)`          | `DecimalType(P=0..38, S=0)`     | `decimal(P=0..38, S=0)`     | `decimal(P=0..38, S=0)`     |
| `decimal(P=0..38, S=0..38)` | `DecimalType(P=0..38, S=0..38)` | `decimal(P=0..38, S=0..38)` | `decimal(P=0..38, S=0..38)` |
| `real`                      | `FloatType()`                   | `real`                      | `real`                      |
| `float`                     | `DoubleType()`                  | `float`                     | `float`                     |
| `smallint`                  | `ShortType()`                   | `smallint`                  | `smallint`                  |
| `tinyint`<br/>`int`                   | `IntegerType()`                 | `int`                       | `int`                       |
| `bigint`                    | `LongType()`                    | `bigint`                    | `bigint`                    |

### Temporal types

!!! note

    MSSQL `timestamp` type is alias for `rowversion` (see `Special types`). It is not a temporal type!

| MSSQL type (read)                        | Spark type                           | MSSQL type (write)                | MSSQL type (create)           |
|------------------------------------------|--------------------------------------|-----------------------------------|-------------------------------|
| `date`                                 | `DateType()`                       | `date`                          | `date`                      |
| `smalldatetime`, minutes<br/>`datetime`, milliseconds<br/>`datetime2(0)`, seconds<br/>`datetime2(3)`, milliseconds               | <br/><br/>`TimestampType()`, microseconds    | <br/><br/>`datetime2(6)`, microseconds    | <br/><br/>`datetime`, milliseconds    |
| `datetime2(6)`, microseconds           | `TimestampType()`, microseconds    | `datetime2(6)`, microseconds    | `datetime`, milliseconds, **precision loss** [3]  |
| `datetime2(7)`, 100s of nanoseconds    | `TimestampType()`, microseconds, **precision loss** [^4]  | `datetime2(6)`, microseconds, **precision loss** [^4]  |                               |
| `time(0)`, seconds<br/>`time(3)`, milliseconds                     | `TimestampType()`, microseconds, with time format quirks [^5]  | `datetime2(6)`, microseconds    | `datetime`, milliseconds    |
| `time(6)`, microseconds                | `TimestampType()`, microseconds,  with time format quirks [^5]  | `datetime2(6)`, microseconds    | `datetime`, milliseconds, **precision loss** [^3]  |
| `time`, 100s of nanoseconds            | `TimestampType()`, microseconds, **precision loss** [^4]  | `datetime2(6)`, microseconds **precision loss** [^3]   |                               |
| `time(7)`, 100s of nanoseconds         | with time format quirks [^5]         |                                   |                               |
| `datetimeoffset`                       | `StringType()`                     | `nvarchar`                      | `nvarchar`                  |

!!! warning

    Note that types in MSSQL and Spark have different value ranges:

    
    | MySQL type        | Min value                      | Max value                      | Spark type          | Min value                      | Max value                      |
    |-------------------|--------------------------------|--------------------------------|---------------------|--------------------------------|--------------------------------|
    | `smalldatetime`<br/>`datetime`<br/>`datetime2`<br/>`time` | `1900-01-01 00:00:00`<br/>`1753-01-01 00:00:00.000`<br/>`0001-01-01 00:00:00.000000`<br/>`00:00:00.0000000`        | `2079-06-06 23:59:00`<br/>`9999-12-31 23:59:59.997`<br/>`9999-12-31 23:59:59.999999`<br/>`23:59:59.9999999`        | `TimestampType()`   | `0001-01-01 00:00:00.000000`   | `9999-12-31 23:59:59.999999`   |

    So not all of values in Spark DataFrame can be written to MSSQL.

    References:

    * [MSSQL date & time types documentation](https://learn.microsoft.com/en-us/sql/t-sql/data-types/date-and-time-types)
    * [Spark DateType documentation](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html)
    * [Spark TimestampType documentation](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html)

[^3]: MSSQL dialect for Spark generates DDL with type `datetime` which has precision up to milliseconds (`23:59:59.999`, 10{superscript}`-3` seconds).
    Inserting data with microsecond and higher precision (`23:59:59.999999` .. `23.59:59.9999999`, 10{superscript}`-6` .. 10{superscript}`-7` seconds)
    will lead to **throwing away microseconds**.

[^4]: MSSQL support timestamp up to 100s of nanoseconds precision (`23:59:59.9999999999`, 10{superscript}`-7` seconds),
    but Spark `TimestampType()` supports datetime up to microseconds precision (`23:59:59.999999`, 10{superscript}`-6` seconds).
    Last digit will be lost during read or write operations.

[^5]: `time` type is the same as `datetime2` with date `1970-01-01`. So instead of reading data from MSSQL like `23:59:59.999999`
    it is actually read `1970-01-01 23:59:59.999999`, and vice versa.

### String types

| MSSQL type (read) | Spark type       | MSSQL type (write) | MSSQL type (create) |
|-------------------|------------------|--------------------|---------------------|
| `char`<br/>`char(N)`<br/>`nchar`<br/>`nchar(N)`<br/>`varchar`<br/>`varchar(N)`<br/>`nvarchar`<br/>`nvarchar(N)`<br/>`mediumtext`<br/>`text`<br/>`ntext`<br/>`xml`          | <br/><br/><br/><br/>`StringType()` | <br/><br/><br/><br/>`nvarchar`       | <br/><br/><br/><br/>`nvarchar`        |

### Binary types

| MSSQL type (read)  | Spark type        | MSSQL type (write) | MSSQL type (create) |
|--------------------|-------------------|--------------------|---------------------|
| `bit`            | `BooleanType()` | `bit`            | `bit`             |
| `binary`<br/>`binary(N)`<br/>`varbinary`<br/>`varbinary(N)`<br/>`image`         | <br/><br/>`BinaryType()`  | <br/><br/>`varbinary`      | <br/><br/>`varbinary`       |

### Special types

| MSSQL type (read)         | Spark type       | MSSQL type (write) | MSSQL type (create) |
|---------------------------|------------------|--------------------|---------------------|
| `geography`<br/>`geometry`<br/>`hierarchyid`<br/>`rowversion`             | <br/><br/>`BinaryType()` | <br/><br/>`varbinary`      | <br/><br/>`varbinary`       |
| `sql_variant`           | unsupported      |                    |                     |
| `sysname`<br/> `uniqueidentifier`              | `StringType()` | `nvarchar`       | `nvarchar`        |

## Explicit type cast

### `DBReader`

It is possible to explicitly cast column type using `DBReader(columns=...)` syntax.

For example, you can use `CAST(column AS text)` to convert data to string representation on MSSQL side, and so it will be read as Spark's `StringType()`:

```python
from onetl.connection import MSSQL
from onetl.db import DBReader

mssql = MSSQL(...)

DBReader(
    connection=mssql,
    columns=[
        "id",
        "supported_column",
        "CAST(unsupported_column AS text) unsupported_column_str",
    ],
)
df = reader.run()

# cast column content to proper Spark type
df = df.select(
    df.id,
    df.supported_column,
    # explicit cast
    df.unsupported_column_str.cast("integer").alias("parsed_integer"),
)
```

### `DBWriter`

Convert dataframe column to JSON using [to_json](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html),
and write it as `text` column in MSSQL:

```python
mssql.execute(
    """
    CREATE TABLE schema.target_tbl (
        id bigint,
        struct_column_json text -- any string type, actually
    )
    """,
)

from pyspark.sql.functions import to_json

df = df.select(
    df.id,
    to_json(df.struct_column).alias("struct_column_json"),
)

writer.run(df)
```

Then you can parse this column on MSSQL side - for example, by creating a view:

```sql
SELECT
    id,
    JSON_VALUE(struct_column_json, "$.nested.field") AS nested_field
FROM target_tbl
```

Or by using [computed column](https://learn.microsoft.com/en-us/sql/relational-databases/tables/specify-computed-columns-in-a-table):

```sql
CREATE TABLE schema.target_table (
    id bigint,
    supported_column datetime2(6),
    struct_column_json text, -- any string type, actually
    -- computed column
    nested_field AS (JSON_VALUE(struct_column_json, "$.nested.field"))
    -- or persisted column
    -- nested_field AS (JSON_VALUE(struct_column_json, "$.nested.field")) PERSISTED
)
```

By default, column value is calculated on every table read.
Column marked as `PERSISTED` is calculated during insert, but this require additional space.
