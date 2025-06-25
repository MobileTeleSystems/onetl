# MySQL <-> Spark type mapping { #mysql-types }

!!! note

    The results below are valid for Spark 3.5.5, and may differ on other Spark versions.

## Type detection & casting

Spark's DataFrames always have a `schema` which is a list of columns with corresponding Spark types. All operations on a column are performed using column type.

### Reading from MySQL

This is how MySQL connector performs this:

- For each column in query result (`SELECT column1, column2, ... FROM table ...`) get column name and MySQL type.
- Find corresponding `MySQL type (read)` → `Spark type` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- Create DataFrame from query with specific column names and Spark types.

### Writing to some existing MySQL table

This is how MySQL connector performs this:

- Get names of columns in DataFrame. [^1]
- Perform `SELECT * FROM table LIMIT 0` query.
- Take only columns present in DataFrame (by name, case insensitive). For each found column get MySQL type.
- Find corresponding `Spark type` → `MySQL type (write)` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- If `MySQL type (write)` match `MySQL type (read)`, no additional casts will be performed, DataFrame column will be written to MySQL as is.
- If `MySQL type (write)` does not match `MySQL type (read)`, DataFrame column will be casted to target column type **on MySQL side**. For example, you can write column with text data to `int` column, if column contains valid integer values within supported value range and precision.

[^1]: This allows to write data to tables with `DEFAULT` and `GENERATED` columns - if DataFrame has no such column,
    it will be populated by MySQL.

### Create new table using Spark

!!! warning

    ABSOLUTELY NOT RECOMMENDED!

This is how MySQL connector performs this:

- Find corresponding `Spark type` → `MySQL type (create)` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- Generate DDL for creating table in MySQL, like `CREATE TABLE (col1 ...)`, and run it.
- Write DataFrame to created table as is.

But some cases this may lead to using wrong column type. For example, Spark creates column of type `timestamp`
which corresponds to MySQL type `timestamp(0)` (precision up to seconds)
instead of more precise `timestamp(6)` (precision up to nanoseconds).
This may lead to incidental precision loss, or sometimes data cannot be written to created table at all.

So instead of relying on Spark to create tables:

??? note "See example"

    ```python

        writer = DBWriter(
            connection=mysql,
            target="myschema.target_tbl",
            options=MySQL.WriteOptions(
                if_exists="append",
                createTableOptions="ENGINE = InnoDB",
            ),
        )
        writer.run(df)
    ```

Always prefer creating tables with specific types **BEFORE WRITING DATA**:

??? note "See example"

    ```python

        mysql.execute(
            """
            CREATE TABLE schema.table (
                id bigint,
                key text,
                value timestamp(6) -- specific type and precision
            )
            ENGINE = InnoDB
            """,
        )

        writer = DBWriter(
            connection=mysql,
            target="myschema.target_tbl",
            options=MySQL.WriteOptions(if_exists="append"),
        )
        writer.run(df)
    ```

### References

Here you can find source code with type conversions:

- [MySQL -> JDBC](https://github.com/mysql/mysql-connector-j/blob/8.0.33/src/main/core-api/java/com/mysql/cj/MysqlType.java#L44-L623)
- [JDBC -> Spark](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/MySQLDialect.scala#L104-L132)
- [Spark -> JDBC](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/MySQLDialect.scala#L204-L211)
- [JDBC -> MySQL](https://github.com/mysql/mysql-connector-j/blob/8.0.33/src/main/core-api/java/com/mysql/cj/MysqlType.java#L625-L867)

## Supported types

See [official documentation](https://dev.mysql.com/doc/refman/en/data-types.html)

### Numeric types

| MySQL type (read)             | Spark type                        | MySQL type (write)            | MySQL type (create)           |
|-------------------------------|-----------------------------------|-------------------------------|-------------------------------|
| `decimal`                   | `DecimalType(P=10, S=0)`        | `decimal(P=10, S=0)`        | `decimal(P=10, S=0)`        |
| `decimal(P=0..38)`          | `DecimalType(P=0..38, S=0)`     | `decimal(P=0..38, S=0)`     | `decimal(P=0..38, S=0)`     |
| `decimal(P=0..38, S=0..30)` | `DecimalType(P=0..38, S=0..30)` | `decimal(P=0..38, S=0..30)` | `decimal(P=0..38, S=0..30)` |
| `decimal(P=39..65, S=...)`  | unsupported [^2]                  |                               |                               |
| `float`<br/>`double`                     | `DoubleType()`                  | `double`                    | `double`                    |
| `tinyint`<br/>`smallint`<br/>`mediumint`<br/>`int`                   | <br/><br/>`IntegerType()`                 | <br/><br/>`int`                       | <br/><br/>`int`                       |
| `bigint`                    | `LongType()`                    | `bigint`                    | `bigint`                    |

[^2]: MySQL support decimal types with precision `P` up to 65.

    But Spark's `DecimalType(P, S)` supports maximum `P=38`. It is impossible to read, write or operate with values of larger precision,
    this leads to an exception.

### Temporal types

| MySQL type (read)                 | Spark type                           | MySQL type (write)                | MySQL type (create)           |
|-----------------------------------|--------------------------------------|-----------------------------------|-------------------------------|
| `year`<br/>`date`                          | `DateType()`                       | `date`                          | `date`                      |
| `datetime`, seconds<br/>`timestamp`, seconds<br/>`datetime(0)`, seconds<br/>`timestamp(0)`, seconds             | <br/><br/>`TimestampType()`, microseconds    | <br/><br/>`timestamp(6)`, microseconds    | <br/><br/>`timestamp(0)`, seconds     |
| `datetime(3)`, milliseconds<br/>`timestamp(3)`, milliseconds<br/>`datetime(6)`, microseconds<br/>`timestamp(6)`, microseconds     | <br/><br/>`TimestampType()`, microseconds    | <br/><br/>`timestamp(6)`, microseconds    | <br/><br/>`timestamp(0)`, seconds, **precision loss** [^3],   |
| `time`, seconds<br/>`time(0)`, seconds                 | `TimestampType()`, microseconds, with time format quirks [^4]  | `timestamp(6)`, microseconds    | `timestamp(0)`, seconds     |
| `time(3)`, milliseconds<br/>`time(6)`, microseconds         | `TimestampType()`, microseconds, with time format quirks [^4]    | `timestamp(6)`, microseconds    | `timestamp(0)`, seconds, **precision loss** [^3],   |

!!! warning

    Note that types in MySQL and Spark have different value ranges:

    
    | MySQL type    | Min value                      | Max value                      | Spark type          | Min value                      | Max value                      |
    |---------------|--------------------------------|--------------------------------|---------------------|--------------------------------|--------------------------------|
    | `year`<br/>`date`      | `1901`<br/>`1000-01-01`                       | `2155`<br/>`9999-12-31`                       | `DateType()`      | `0001-01-01`                 | `9999-12-31`                 |
    | `datetime`<br/>`timestamp`<br/>`time`  | `1000-01-01 00:00:00.000000`<br/>`1970-01-01 00:00:01.000000`<br/>`-838:59:59.000000` | `9999-12-31 23:59:59.499999`<br/>`9999-12-31 23:59:59.499999`<br/>`838:59:59.000000` | `TimestampType()` | `0001-01-01 00:00:00.000000` | `9999-12-31 23:59:59.999999` |

    So Spark can read all the values from MySQL, but not all of values in Spark DataFrame can be written to MySQL.

    References:

    * [MySQL year documentation](https://dev.mysql.com/doc/refman/en/year.html)
    * [MySQL date, datetime & timestamp documentation](https://dev.mysql.com/doc/refman/en/datetime.html)
    * [MySQL time documentation](https://dev.mysql.com/doc/refman/en/time.html)
    * [Spark DateType documentation](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html)
    * [Spark TimestampType documentation](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html)

[^3]: MySQL dialect generates DDL with MySQL type `timestamp` which is alias for `timestamp(0)` with precision up to seconds (`23:59:59`).
    Inserting data with microseconds precision (`23:59:59.999999`) will lead to **throwing away microseconds**.

[^4]: `time` type is the same as `timestamp` with date `1970-01-01`. So instead of reading data from MySQL like `23:59:59`
    it is actually read `1970-01-01 23:59:59`, and vice versa.

### String types

| MySQL type (read)             | Spark type       | MySQL type (write) | MySQL type (create) |
|-------------------------------|------------------|--------------------|---------------------|
| `char`<br/>`char(N)`<br/>`varchar(N)`<br/>`mediumtext`<br/>`text`<br/>`longtext`<br/>`json`<br/>`enum("val1", "val2", ...)`<br/>`set("val1", "val2", ...)`  | <br/><br/><br/><br/>`StringType()` | <br/><br/><br/><br/>`longtext`       | <br/><br/><br/><br/>`longtext`        |

### Binary types

| MySQL type (read) | Spark type       | MySQL type (write) | MySQL type (create) |
|-------------------|------------------|--------------------|---------------------|
| `binary`<br/>`binary(N)`<br/>`varbinary(N)`<br/>`mediumblob`<br/>`blob`<br/>`longblob`        | <br/><br/><br/>`BinaryType()` | <br/><br/><br/>`blob`           | <br/><br/><br/>`blob`            |

### Geometry types

| MySQL type (read)      | Spark type       | MySQL type (write) | MySQL type (create) |
|------------------------|------------------|--------------------|---------------------|
| `point`<br/>`linestring`<br/>`polygon`<br/>`geometry`<br/>`multipoint`<br/>`multilinestring`<br/>`multipolygon`<br/>`geometrycollection`              | <br/><br/><br/><br/>`BinaryType()` | <br/><br/><br/><br/>`blob`           | <br/><br/><br/><br/>`blob`            |

## Explicit type cast

### `DBReader`

It is possible to explicitly cast column type using `DBReader(columns=...)` syntax.

For example, you can use `CAST(column AS text)` to convert data to string representation on MySQL side, and so it will be read as Spark's `StringType()`.

It is also possible to use [JSON_OBJECT](https://dev.mysql.com/doc/refman/en/json.html) MySQL function and parse JSON columns in MySQL with the [JSON.parse_column][onetl.file.format.json.JSON.parse_column] method.

```python
from pyspark.sql.types import IntegerType, StructType, StructField

from onetl.connection import MySQL
from onetl.db import DBReader
from onetl.file.format import JSON

mysql = MySQL(...)

DBReader(
    connection=mysql,
    columns=[
        "id",
        "supported_column",
        "CAST(unsupported_column AS text) unsupported_column_str",
        # or
        "JSON_OBJECT('key', value_column) json_column",
    ],
)
df = reader.run()

json_scheme = StructType([StructField("key", IntegerType())])

df = df.select(
    df.id,
    df.supported_column,
    # explicit cast
    df.unsupported_column_str.cast("integer").alias("parsed_integer"),
    JSON().parse_column("json_column", json_scheme).alias("struct_column"),
)
```

### `DBWriter`

To write JSON data to a `json` or `text` column in a MySQL table, use the [JSON.serialize_column][onetl.file.format.json.JSON.serialize_column] method.

```python
from onetl.connection import MySQL
from onetl.db import DBWriter
from onetl.file.format import JSON

mysql.execute(
    """
    CREATE TABLE schema.target_tbl (
        id bigint,
        array_column_json json -- any string type, actually
    )
    ENGINE = InnoDB
    """,
)

df = df.select(
    df.id,
    JSON().serialize_column(df.array_column).alias("array_column_json"),
)

writer.run(df)
```

Then you can parse this column on MySQL side - for example, by creating a view:

```sql
SELECT
    id,
    array_column_json->"$[0]" AS array_item
FROM target_tbl
```

Or by using [GENERATED column](https://dev.mysql.com/doc/refman/en/create-table-generated-columns.html):

```sql
CREATE TABLE schema.target_table (
    id bigint,
    supported_column timestamp,
    array_column_json json, -- any string type, actually
    -- virtual column
    array_item_0 GENERATED ALWAYS AS (array_column_json->"$[0]")) VIRTUAL
    -- or stired column
    -- array_item_0 GENERATED ALWAYS AS (array_column_json->"$[0]")) STORED
)
```

`VIRTUAL` column value is calculated on every table read.
`STORED` column value is calculated during insert, but this require additional space.
