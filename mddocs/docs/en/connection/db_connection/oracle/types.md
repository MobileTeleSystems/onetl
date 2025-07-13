# Oracle <-> Spark type mapping { #oracle-types }

!!! note

    The results below are valid for Spark 3.5.5, and may differ on other Spark versions.

## Type detection & casting

Spark's DataFrames always have a `schema` which is a list of columns with corresponding Spark types. All operations on a column are performed using column type.

### Reading from Oracle

This is how Oracle connector performs this:

- For each column in query result (`SELECT column1, column2, ... FROM table ...`) get column name and Oracle type.
- Find corresponding `Oracle type (read)` → `Spark type` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- Create DataFrame from query with specific column names and Spark types.

### Writing to some existing Oracle table

This is how Oracle connector performs this:

- Get names of columns in DataFrame. [^1]
- Perform `SELECT * FROM table LIMIT 0` query.
- Take only columns present in DataFrame (by name, case insensitive). For each found column get Clickhouse type.
- **Find corresponding** `Oracle type (read)` → `Spark type` **combination** (see below) for each DataFrame column. If no combination is found, raise exception. [^2]
- Find corresponding `Spark type` → `Oracle type (write)` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- If `Oracle type (write)` match `Oracle type (read)`, no additional casts will be performed, DataFrame column will be written to Oracle as is.
- If `Oracle type (write)` does not match `Oracle type (read)`, DataFrame column will be casted to target column type **on Oracle side**.
  For example, you can write column with text data to `int` column, if column contains valid integer values within supported value range and precision.

[^1]: This allows to write data to tables with `DEFAULT` and `GENERATED` columns - if DataFrame has no such column, it will be populated by Oracle.

[^2]: Yes, this is weird.

### Create new table using Spark

!!! warning

    ABSOLUTELY NOT RECOMMENDED!S

This is how Oracle connector performs this:

- Find corresponding `Spark type` → `Oracle type (create)` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- Generate DDL for creating table in Oracle, like `CREATE TABLE (col1 ...)`, and run it.
- Write DataFrame to created table as is.

But Oracle connector support only limited number of types and almost no custom clauses (like `PARTITION BY`, `INDEX`, etc).
So instead of relying on Spark to create tables:

??? note "See example"

    ```python

        writer = DBWriter(
            connection=oracle,
            target="public.table",
            options=Oracle.WriteOptions(if_exists="append"),
        )
        writer.run(df)
    ```

Always prefer creating table with desired DDL **BEFORE WRITING DATA**:

??? note "See example"

    ```python

        oracle.execute(
            """
            CREATE TABLE username.table (
                id NUMBER,
                business_dt TIMESTAMP(6),
                value VARCHAR2(2000)
            )
            """,
        )

        writer = DBWriter(
            connection=oracle,
            target="public.table",
            options=Oracle.WriteOptions(if_exists="append"),
        )
        writer.run(df)
    ```

See Oracle [CREATE TABLE](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/SELECT.html) documentation.

## Supported types

### References

See [List of Oracle types](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Data-Types.html).

Here you can find source code with type conversions:

- [JDBC -> Spark](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/OracleDialect.scala#L83-L109)
- [Spark -> JDBC](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/OracleDialect.scala#L111-L123)

### Numeric types

| Oracle type (read)               | Spark type                        | Oracle type (write)           | Oracle type (create)      |
|----------------------------------|-----------------------------------|-------------------------------|---------------------------|
| `NUMBER`                       | `DecimalType(P=38, S=10)`       | `NUMBER(P=38, S=10)`        | `NUMBER(P=38, S=10)`    |
| `NUMBER(P=0..38)`              | `DecimalType(P=0..38, S=0)`     | `NUMBER(P=0..38, S=0)`      | `NUMBER(P=38, S=0)`     |
| `NUMBER(P=0..38, S=0..38)`     | `DecimalType(P=0..38, S=0..38)` | `NUMBER(P=0..38, S=0..38)`  | `NUMBER(P=38, S=0..38)` |
| `NUMBER(P=..., S=-127..-1)`    | unsupported [^3]                  |                               |                           |
| `FLOAT`<br/>`FLOAT(N)`<br/>`REAL`<br/>`DOUBLE PRECISION` | <br/>`DecimalType(P=38, S=10)`       | <br/>`NUMBER(P=38, S=10)`        | <br/>`NUMBER(P=38, S=10)`    |
| `BINARY_FLOAT`<br/>`BINARY_DOUBLE`                 | `FloatType()`<br/>`DoubleType()`                   | `NUMBER(P=19, S=4)`         | `NUMBER(P=19, S=4)`     |
| `SMALLINT`<br/>`INTEGER`       | `DecimalType(P=38, S=0)`        | `NUMBER(P=38, S=0)`         | `NUMBER(P=38, S=0)`     |
| `LONG`                         | `StringType()`                  | `CLOB`                      | `CLOB`                  |

[^3]: Oracle support decimal types with negative scale, like `NUMBER(38, -10)`. Spark doesn't.

### Temporal types

| Oracle type (read)                         | Spark type                         | Oracle type (write)             | Oracle type (create)            |
|--------------------------------------------|------------------------------------|---------------------------------|---------------------------------|
| `DATE`, days                             | `TimestampType()`, microseconds  | `TIMESTAMP(6)`, microseconds  | `TIMESTAMP(6)`, microseconds  |
| `TIMESTAMP`, microseconds<br/>`TIMESTAMP(0)`, seconds<br/>`TIMESTAMP(3)`, milliseconds<br/>`TIMESTAMP(6)`, microseconds               | <br/><br/>`TimestampType()`, microseconds  | <br/><br/>`TIMESTAMP(6)`, microseconds  | <br/><br/>`TIMESTAMP(6)`, microseconds **precision loss** |
| `TIMESTAMP(9)`, nanoseconds              | `TimestampType()`, microseconds, **precision loss** [^4] | `TIMESTAMP(6)`, microseconds, **precision loss** | `TIMESTAMP(6)`, microseconds, |
| `TIMESTAMP WITH TIME ZONE`<br/>`TIMESTAMP(N) WITH TIME ZONE`<br/>`TIMESTAMP WITH LOCAL TIME ZONE`<br/>`TIMESTAMP(N) WITH LOCAL TIME ZONE`<br/>`INTERVAL YEAR TO MONTH`<br/>`INTERVAL DAY TO SECOND`               | <br/><br/><br/>unsupported                        |                                 |                                 |

!!! warning

    Note that types in Oracle and Spark have different value ranges:

    | Oracle type   | Min value                          | Max value                         | Spark type          | Min value                      | Max value                      |
    |---------------|------------------------------------|-----------------------------------|---------------------|--------------------------------|--------------------------------|
    | `date`      | `-4712-01-01`                    | `9999-01-01`                    | `DateType()`      | `0001-01-01`                 | `9999-12-31`                 |
    | `timestamp` | `-4712-01-01 00:00:00.000000000` | `9999-12-31 23:59:59.999999999` | `TimestampType()` | `0001-01-01 00:00:00.000000` | `9999-12-31 23:59:59.999999` |

    So not all of values can be read from Oracle to Spark.

    References:

    * [Oracle date, timestamp and intervals documentation](https://oracle-base.com/articles/misc/oracle-dates-timestamps-and-intervals#DATE)
    * [Spark DateType documentation](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html)
    * [Spark TimestampType documentation](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html)

[^4]: Oracle support timestamp up to nanoseconds precision (`23:59:59.999999999`), but Spark `TimestampType()` supports datetime up to microseconds precision (`23:59:59.999999`). Nanoseconds will be lost during read or write operations.

### String types

| Oracle type (read)          | Spark type       | Oracle type (write) | Oracle type (create) |
|-----------------------------|------------------|---------------------|----------------------|
| `CHAR`<br/>`CHAR(N CHAR)`<br/>`CHAR(N BYTE)`<br/>`NCHAR`<br/>`NCHAR(N)`<br/>`VARCHAR(N)`<br/>`LONG VARCHAR`<br/>`VARCHAR2(N CHAR)`<br/>`VARCHAR2(N BYTE)`<br/>`NVARCHAR2(N)`<br/>`CLOB`<br/>`NCLOB`                    | <br/><br/><br/><br/><br/>`StringType()` | <br/><br/><br/><br/><br/>`CLOB`            | <br/><br/><br/><br/><br/>`CLOB`             |

### Binary types

| Oracle type (read)       | Spark type       | Oracle type (write) | Oracle type (create) |
|--------------------------|------------------|---------------------|----------------------|
| `RAW(N)`<br/>`LONG RAW`<br/>`BLOB`               | <br/>`BinaryType()` | <br/>`BLOB`            | <br/>`BLOB`             |
| `BFILE`                | unsupported      |                     |                      |

### Struct types

| Oracle type (read)                  | Spark type       | Oracle type (write) | Oracle type (create) |
|-------------------------------------|------------------|---------------------|----------------------|
| `XMLType`<br/>`URIType`<br/>`DBURIType`<br/>`XDBURIType`<br/>`HTTPURIType`<br/>`CREATE TYPE ... AS OBJECT (...)`                         | `StringType()` | `CLOB`            | `CLOB`             |
| `JSON`<br/>`CREATE TYPE ... AS VARRAY ...`<br/>`CREATE TYPE ... AS TABLE OF ...`                            | <br/>unsupported      |                     |                      |

### Special types

| Oracle type (read) | Spark type        | Oracle type (write) | Oracle type (create) |
|--------------------|-------------------|---------------------|----------------------|
| `BOOLEAN`        | `BooleanType()` | `BOOLEAN`         | `NUMBER(P=1, S=0)` |
| `ROWID`<br/>`UROWID`<br/>`UROWID(N)`          | <br/>`StringType()`  | <br/>`CLOB`            | <br/>`CLOB`             |
| `ANYTYPE`<br/>`ANYDATA`<br/>`ANYDATASET`        | <br/>unsupported       |                     |                      |

## Explicit type cast

### `DBReader`

It is possible to explicitly cast column of unsupported type using `DBReader(columns=...)` syntax.

For example, you can use `CAST(column AS CLOB)` to convert data to string representation on Oracle side, and so it will be read as Spark's `StringType()`.

It is also possible to use [JSON_ARRAY](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/JSON_ARRAY.html) or [JSON_OBJECT](https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/JSON_OBJECT.html) Oracle functions to convert column of any type to string representation. Then this JSON string can then be effectively parsed using the [JSON.parse_column][onetl.file.format.json.JSON.parse_column] method.

    ```python
        from onetl.file.format import JSON
        from pyspark.sql.types import IntegerType, StructType, StructField

        from onetl.connection import Oracle
        from onetl.db import DBReader

        oracle = Oracle(...)

        DBReader(
            connection=oracle,
            columns=[
                "id",
                "supported_column",
                "CAST(unsupported_column AS VARCHAR2(4000)) unsupported_column_str",
                # or
                "JSON_ARRAY(array_column) array_column_json",
            ],
        )
        df = reader.run()

        json_scheme = StructType([StructField("key", IntegerType())])

        df = df.select(
            df.id,
            df.supported_column,
            df.unsupported_column_str.cast("integer").alias("parsed_integer"),
            JSON().parse_column("array_column_json", json_scheme).alias("array_column"),
        )
    ```

### `DBWriter`

It is always possible to convert data on Spark side to string, and then write it to text column in Oracle table.

To serialize and write JSON data to a `text` or `json` column in an Oracle table use the [JSON.serialize_column][onetl.file.format.json.JSON.serialize_column] method.

    ```python
        from onetl.connection import Oracle
        from onetl.db import DBWriter
        from onetl.file.format import JSON

        oracle = Oracle(...)

        oracle.execute(
            """
            CREATE TABLE schema.target_table (
                id INTEGER,
                supported_column TIMESTAMP,
                array_column_json VARCHAR2(4000) -- any string type, actually
            )
            """,
        )

        write_df = df.select(
            df.id,
            df.supported_column,
            JSON().serialize_column(df.unsupported_column).alias("array_column_json"),
        )

        writer = DBWriter(
            connection=oracle,
            target="schema.target_table",
        )
        writer.run(write_df)
    ```

Then you can parse this column on Oracle side - for example, by creating a view:

    ```sql
        SELECT
            id,
            supported_column,
            JSON_VALUE(array_column_json, '$[0]' RETURNING NUMBER) AS array_item_0
        FROM
            schema.target_table
    ```

Or by using [VIRTUAL column](https://oracle-base.com/articles/11g/virtual-columns-11gr1):

    ```sql
        CREATE TABLE schema.target_table (
            id INTEGER,
            supported_column TIMESTAMP,
            array_column_json VARCHAR2(4000), -- any string type, actually
            array_item_0 GENERATED ALWAYS AS (JSON_VALUE(array_column_json, '$[0]' RETURNING NUMBER)) VIRTUAL
        )
    ```

But data will be parsed on each table read in any case, as Oracle does no support `GENERATED ALWAYS AS (...) STORED` columns.
