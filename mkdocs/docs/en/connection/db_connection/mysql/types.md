(mysql-types)=

# MySQL \<-> Spark type mapping

```{eval-rst}
.. note::

    The results below are valid for Spark 3.5.5, and may differ on other Spark versions.
```

## Type detection & casting

Spark's DataFrames always have a `schema` which is a list of columns with corresponding Spark types. All operations on a column are performed using column type.

### Reading from MySQL

This is how MySQL connector performs this:

- For each column in query result (`SELECT column1, column2, ... FROM table ...`) get column name and MySQL type.
- Find corresponding `MySQL type (read)` → `Spark type` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- Create DataFrame from query with specific column names and Spark types.

### Writing to some existing MySQL table

This is how MySQL connector performs this:

- Get names of columns in DataFrame. [^footnote-1]
- Perform `SELECT * FROM table LIMIT 0` query.
- Take only columns present in DataFrame (by name, case insensitive). For each found column get MySQL type.
- Find corresponding `Spark type` → `MySQL type (write)` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- If `MySQL type (write)` match `MySQL type (read)`, no additional casts will be performed, DataFrame column will be written to MySQL as is.
- If `MySQL type (write)` does not match `MySQL type (read)`, DataFrame column will be casted to target column type **on MySQL side**. For example, you can write column with text data to `int` column, if column contains valid integer values within supported value range and precision.

[^footnote-1]: This allows to write data to tables with `DEFAULT` and `GENERATED` columns - if DataFrame has no such column,
    it will be populated by MySQL.

### Create new table using Spark

```{eval-rst}
.. warning::

    ABSOLUTELY NOT RECOMMENDED!
```

This is how MySQL connector performs this:

- Find corresponding `Spark type` → `MySQL type (create)` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- Generate DDL for creating table in MySQL, like `CREATE TABLE (col1 ...)`, and run it.
- Write DataFrame to created table as is.

But some cases this may lead to using wrong column type. For example, Spark creates column of type `timestamp`
which corresponds to MySQL type `timestamp(0)` (precision up to seconds)
instead of more precise `timestamp(6)` (precision up to nanoseconds).
This may lead to incidental precision loss, or sometimes data cannot be written to created table at all.

So instead of relying on Spark to create tables:

```{eval-rst}
.. dropdown:: See example

    .. code:: python

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

```{eval-rst}
.. dropdown:: See example

    .. code:: python

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

```{eval-rst}
+-------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| MySQL type (read)             | Spark type                        | MySQL type (write)            | MySQL type (create)           |
+===============================+===================================+===============================+===============================+
| `decimal`                   | `DecimalType(P=10, S=0)`        | `decimal(P=10, S=0)`        | `decimal(P=10, S=0)`        |
+-------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| `decimal(P=0..38)`          | `DecimalType(P=0..38, S=0)`     | `decimal(P=0..38, S=0)`     | `decimal(P=0..38, S=0)`     |
+-------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| `decimal(P=0..38, S=0..30)` | `DecimalType(P=0..38, S=0..30)` | `decimal(P=0..38, S=0..30)` | `decimal(P=0..38, S=0..30)` |
+-------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| `decimal(P=39..65, S=...)`  | unsupported [2]_                  |                               |                               |
+-------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| `float`                     | `DoubleType()`                  | `double`                    | `double`                    |
+-------------------------------+                                   |                               |                               |
| `double`                    |                                   |                               |                               |
+-------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| `tinyint`                   | `IntegerType()`                 | `int`                       | `int`                       |
+-------------------------------+                                   |                               |                               |
| `smallint`                  |                                   |                               |                               |
+-------------------------------+                                   |                               |                               |
| `mediumint`                 |                                   |                               |                               |
+-------------------------------+                                   |                               |                               |
| `int`                       |                                   |                               |                               |
+-------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| `bigint`                    | `LongType()`                    | `bigint`                    | `bigint`                    |
+-------------------------------+-----------------------------------+-------------------------------+-------------------------------+
```

[^footnote-2]: MySQL support decimal types with precision `P` up to 65.

    But Spark's `DecimalType(P, S)` supports maximum `P=38`. It is impossible to read, write or operate with values of larger precision,
    this leads to an exception.

### Temporal types

```{eval-rst}
+-----------------------------------+--------------------------------------+-----------------------------------+-------------------------------+
| MySQL type (read)                 | Spark type                           | MySQL type (write)                | MySQL type (create)           |
+===================================+======================================+===================================+===============================+
| `year`                          | `DateType()`                       | `date`                          | `date`                      |
+-----------------------------------+                                      |                                   |                               |
| `date`                          |                                      |                                   |                               |
+-----------------------------------+--------------------------------------+-----------------------------------+-------------------------------+
| `datetime`, seconds             | `TimestampType()`, microseconds    | `timestamp(6)`, microseconds    | `timestamp(0)`, seconds     |
+-----------------------------------+                                      |                                   |                               |
| `timestamp`, seconds            |                                      |                                   |                               |
+-----------------------------------+                                      |                                   |                               |
| `datetime(0)`, seconds          |                                      |                                   |                               |
+-----------------------------------+                                      |                                   |                               |
| `timestamp(0)`, seconds         |                                      |                                   |                               |
+-----------------------------------+--------------------------------------+-----------------------------------+-------------------------------+
| `datetime(3)`, milliseconds     | `TimestampType()`, microseconds    | `timestamp(6)`, microseconds    | `timestamp(0)`, seconds,    |
+-----------------------------------+                                      |                                   | **precision loss** [4]_,      |
| `timestamp(3)`, milliseconds    |                                      |                                   |                               |
+-----------------------------------+                                      |                                   |                               |
| `datetime(6)`, microseconds     |                                      |                                   |                               |
+-----------------------------------+                                      |                                   |                               |
| `timestamp(6)`, microseconds    |                                      |                                   |                               |
+-----------------------------------+--------------------------------------+-----------------------------------+-------------------------------+
| `time`, seconds                 | `TimestampType()`, microseconds,   | `timestamp(6)`, microseconds    | `timestamp(0)`, seconds     |
+-----------------------------------+ with time format quirks [5]_         |                                   |                               |
| `time(0)`, seconds              |                                      |                                   |                               |
+-----------------------------------+--------------------------------------+-----------------------------------+-------------------------------+
| `time(3)`, milliseconds         | `TimestampType()`, microseconds    | `timestamp(6)`, microseconds    | `timestamp(0)`, seconds,    |
+-----------------------------------+ with time format quirks [5]_         |                                   | **precision loss** [4]_,      |
| `time(6)`, microseconds         |                                      |                                   |                               |
+-----------------------------------+--------------------------------------+-----------------------------------+-------------------------------+
```

```{eval-rst}
.. warning::

    Note that types in MySQL and Spark have different value ranges:

    +---------------+--------------------------------+--------------------------------+---------------------+--------------------------------+--------------------------------+
    | MySQL type    | Min value                      | Max value                      | Spark type          | Min value                      | Max value                      |
    +===============+================================+================================+=====================+================================+================================+
    | `year`      | `1901`                       | `2155`                       | `DateType()`      | `0001-01-01`                 | `9999-12-31`                 |
    +---------------+--------------------------------+--------------------------------+                     |                                |                                |
    | `date`      | `1000-01-01`                 | `9999-12-31`                 |                     |                                |                                |
    +---------------+--------------------------------+--------------------------------+---------------------+--------------------------------+--------------------------------+
    | `datetime`  | `1000-01-01 00:00:00.000000` | `9999-12-31 23:59:59.499999` | `TimestampType()` | `0001-01-01 00:00:00.000000` | `9999-12-31 23:59:59.999999` |
    +---------------+--------------------------------+--------------------------------+                     |                                |                                |
    | `timestamp` | `1970-01-01 00:00:01.000000` | `9999-12-31 23:59:59.499999` |                     |                                |                                |
    +---------------+--------------------------------+--------------------------------+                     |                                |                                |
    | `time`      | `-838:59:59.000000`          | `838:59:59.000000`           |                     |                                |                                |
    +---------------+--------------------------------+--------------------------------+---------------------+--------------------------------+--------------------------------+

    So Spark can read all the values from MySQL, but not all of values in Spark DataFrame can be written to MySQL.

    References:
        * `MySQL year documentation <https://dev.mysql.com/doc/refman/en/year.html>`_
        * `MySQL date, datetime & timestamp documentation <https://dev.mysql.com/doc/refman/en/datetime.html>`_
        * `MySQL time documentation <https://dev.mysql.com/doc/refman/en/time.html>`_
        * `Spark DateType documentation <https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html>`_
        * `Spark TimestampType documentation <https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html>`_
```

[^footnote-3]: MySQL dialect generates DDL with MySQL type `timestamp` which is alias for `timestamp(0)` with precision up to seconds (`23:59:59`).
    Inserting data with microseconds precision (`23:59:59.999999`) will lead to **throwing away microseconds**.

[^footnote-4]: `time` type is the same as `timestamp` with date `1970-01-01`. So instead of reading data from MySQL like `23:59:59`
    it is actually read `1970-01-01 23:59:59`, and vice versa.

### String types

```{eval-rst}
+-------------------------------+------------------+--------------------+---------------------+
| MySQL type (read)             | Spark type       | MySQL type (write) | MySQL type (create) |
+===============================+==================+====================+=====================+
| `char`                      | `StringType()` | `longtext`       | `longtext`        |
+-------------------------------+                  |                    |                     |
| `char(N)`                   |                  |                    |                     |
+-------------------------------+                  |                    |                     |
| `varchar(N)`                |                  |                    |                     |
+-------------------------------+                  |                    |                     |
| `mediumtext`                |                  |                    |                     |
+-------------------------------+                  |                    |                     |
| `text`                      |                  |                    |                     |
+-------------------------------+                  |                    |                     |
| `longtext`                  |                  |                    |                     |
+-------------------------------+                  |                    |                     |
| `json`                      |                  |                    |                     |
+-------------------------------+                  |                    |                     |
| `enum("val1", "val2", ...)` |                  |                    |                     |
+-------------------------------+                  |                    |                     |
| `set("val1", "val2", ...)`  |                  |                    |                     |
+-------------------------------+------------------+--------------------+---------------------+
```

### Binary types

```{eval-rst}
+-------------------+------------------+--------------------+---------------------+
| MySQL type (read) | Spark type       | MySQL type (write) | MySQL type (create) |
+===================+==================+====================+=====================+
| `binary`        | `BinaryType()` | `blob`           | `blob`            |
+-------------------+                  |                    |                     |
| `binary(N)`     |                  |                    |                     |
+-------------------+                  |                    |                     |
| `varbinary(N)`  |                  |                    |                     |
+-------------------+                  |                    |                     |
| `mediumblob`    |                  |                    |                     |
+-------------------+                  |                    |                     |
| `blob`          |                  |                    |                     |
+-------------------+                  |                    |                     |
| `longblob`      |                  |                    |                     |
+-------------------+------------------+--------------------+---------------------+
```

### Geometry types

```{eval-rst}
+------------------------+------------------+--------------------+---------------------+
| MySQL type (read)      | Spark type       | MySQL type (write) | MySQL type (create) |
+========================+==================+====================+=====================+
| `point`              | `BinaryType()` | `blob`           | `blob`            |
+------------------------+                  |                    |                     |
| `linestring`         |                  |                    |                     |
+------------------------+                  |                    |                     |
| `polygon`            |                  |                    |                     |
+------------------------+                  |                    |                     |
| `geometry`           |                  |                    |                     |
+------------------------+                  |                    |                     |
| `multipoint`         |                  |                    |                     |
+------------------------+                  |                    |                     |
| `multilinestring`    |                  |                    |                     |
+------------------------+                  |                    |                     |
| `multipolygon`       |                  |                    |                     |
+------------------------+                  |                    |                     |
| `geometrycollection` |                  |                    |                     |
+------------------------+------------------+--------------------+---------------------+
```

## Explicit type cast

### `DBReader`

It is possible to explicitly cast column type using `DBReader(columns=...)` syntax.

For example, you can use `CAST(column AS text)` to convert data to string representation on MySQL side, and so it will be read as Spark's `StringType()`.

It is also possible to use [JSON_OBJECT](https://dev.mysql.com/doc/refman/en/json.html) MySQL function and parse JSON columns in MySQL with the {obj}`JSON.parse_column <onetl.file.format.json.JSON.parse_column>` method.

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

To write JSON data to a `json` or `text` column in a MySQL table, use the {obj}`JSON.serialize_column <onetl.file.format.json.JSON.serialize_column>` method.

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
