# Clickhouse \<-> Spark type mapping { #clickhouse-types }

!!! note

    The results below are valid for Spark 3.5.5, and may differ on other Spark versions.

!!! note

    It is recommended to use `spark-dialect-extension <https://github.com/MobileTeleSystems/spark-dialect-extension>`_ package,
    which implements writing Arrays from Spark to Clickhouse, fixes dropping fractions of seconds in `TimestampType`,
    and fixes other type conversion issues.

## Type detection & casting

Spark's DataFrames always have a `schema` which is a list of columns with corresponding Spark types. All operations on a column are performed using column type.

### Reading from Clickhouse

This is how Clickhouse connector performs this:

- For each column in query result (`SELECT column1, column2, ... FROM table ...`) get column name and Clickhouse type.
- Find corresponding `Clickhouse type (read)` → `Spark type` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- Create DataFrame from query with specific column names and Spark types.

### Writing to some existing Clickhouse table

This is how Clickhouse connector performs this:

- Get names of columns in DataFrame. [^1]
- Perform `SELECT * FROM table LIMIT 0` query.
- Take only columns present in DataFrame (by name, case insensitive). For each found column get Clickhouse type.
- **Find corresponding** `Clickhouse type (read)` → `Spark type` **combination** (see below) for each DataFrame column. If no combination is found, raise exception. [^2]
- Find corresponding `Spark type` → `Clickhousetype (write)` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- If `Clickhousetype (write)` match `Clickhouse type (read)`, no additional casts will be performed, DataFrame column will be written to Clickhouse as is.
- If `Clickhousetype (write)` does not match `Clickhouse type (read)`, DataFrame column will be casted to target column type **on Clickhouse side**. For example, you can write column with text data to `Int32` column, if column contains valid integer values within supported value range and precision.

[^1]: This allows to write data to tables with `DEFAULT` columns - if DataFrame has no such column, it will be populated by Clickhouse.

[^2]: Yes, this is weird.

### Create new table using Spark

!!! warning

    ABSOLUTELY NOT RECOMMENDED!

This is how Clickhouse connector performs this:

- Find corresponding `Spark type` → `Clickhouse type (create)` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- Generate DDL for creating table in Clickhouse, like `CREATE TABLE (col1 ...)`, and run it.
- Write DataFrame to created table as is.

But Spark does not have specific dialect for Clickhouse, so Generic JDBC dialect is used.
Generic dialect is using SQL ANSI type names while creating tables in target database, not database-specific types.

If some cases this may lead to using wrong column type. For example, Spark creates column of type `TIMESTAMP`
which corresponds to Clickhouse type `DateTime32` (precision up to seconds)
instead of more precise `DateTime64` (precision up to nanoseconds).
This may lead to incidental precision loss, or sometimes data cannot be written to created table at all.

So instead of relying on Spark to create tables:

??? "See example"

    ```python

        writer = DBWriter(
            connection=clickhouse,
            target="default.target_tbl",
            options=Clickhouse.WriteOptions(
                if_exists="append",
                # ENGINE is required by Clickhouse
                createTableOptions="ENGINE = MergeTree() ORDER BY id",
            ),
        )
        writer.run(df)
    ```

Always prefer creating tables with specific types **BEFORE WRITING DATA**:

??? "See example"

    ```python

        clickhouse.execute(
            """
            CREATE TABLE default.target_tbl (
                id UInt8,
                value DateTime64(6) -- specific type and precision
            )
            ENGINE = MergeTree()
            ORDER BY id
            """,
        )

        writer = DBWriter(
            connection=clickhouse,
            target="default.target_tbl",
            options=Clickhouse.WriteOptions(if_exists="append"),
        )
        writer.run(df)
    ```

### References

Here you can find source code with type conversions:

- [Clickhouse -> JDBC](https://github.com/ClickHouse/clickhouse-java/blob/0.3.2/clickhouse-jdbc/src/main/java/com/clickhouse/jdbc/JdbcTypeMapping.java#L39-L176)
- [JDBC -> Spark](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L307)
- [Spark -> JDBC](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L141-L164)
- [JDBC -> Clickhouse](https://github.com/ClickHouse/clickhouse-java/blob/0.3.2/clickhouse-jdbc/src/main/java/com/clickhouse/jdbc/JdbcTypeMapping.java#L185-L311)

## Supported types

See [official documentation](https://clickhouse.com/docs/en/sql-reference/data-types)

### Generic types

- `LowCardinality(T)` is same as `T`
- `Nullable(T)` is same as `T`, but Spark column is inferred as `nullable=True`

### Numeric types


| Clickhouse type (read)         | Spark type                        | Clickhouse type (write)       | Clickhouse type (create)    
|--------------------------------|-----------------------------------|-------------------------------|-----------------------------
| `Bool`                         | `BooleanType()`                 | `Bool`                      | `UInt64`                   
| `Decimal`                      | `DecimalType(P=10, S=0)`        | `Decimal(P=10, S=0)`        | `Decimal(P=10, S=0)`       
| `Decimal(P=0..38)`             | `DecimalType(P=0..38, S=0)`     | `Decimal(P=0..38, S=0)`     | `Decimal(P=0..38, S=0)`    
| `Decimal(P=0..38, S=0..38)`    | `DecimalType(P=0..38, S=0..38)` | `Decimal(P=0..38, S=0..38)` | `Decimal(P=0..38, S=0..38)`
| `Decimal(P=39..76, S=0..76)`   | unsupported [^3]               |                             |                             
| `Decimal32(P=0..9)`            | `DecimalType(P=9, S=0..9)`      | `Decimal(P=9, S=0..9)`      | `Decimal(P=9, S=0..9)`     
| `Decimal64(S=0..18)`           | `DecimalType(P=18, S=0..18)`    | `Decimal(P=18, S=0..18)`    | `Decimal(P=18, S=0..18)`   
| `Decimal128(S=0..38)`          | `DecimalType(P=38, S=0..38)`    | `Decimal(P=38, S=0..38)`    | `Decimal(P=38, S=0..38)`   
| `Decimal256(S=0..76)`          | unsupported [^3]               |                             |                             
| `Float32`                      | `FloatType()`                   | `Float32`                   | `Float32`                  
| `Float64`                      | `DoubleType()`                  | `Float64`                   | `Float64`                  
| `Int8`<br/>`Int16`<br/>`Int32` | <br/>`IntegerType()`            | <br/>`Int32`                | <br/>`Int32`
| `Int64`                        | `LongType()`                    | `Int64`                     | `Int64`                    
| `Int128`<br/>`Int256`          | unsupported [^3]               |                             |                             
| `-`                            | `ByteType()`                    | `Int8`                      | `Int8`                     
| `-`                            | `ShortType()`                   | `Int32`                     | `Int32`                    
| `UInt8`                        | `IntegerType()`                 | `Int32`                     | `Int32`                    
| `UInt16`                       | `LongType()`                    | `Int64`                     | `Int64`                    
| `UInt32`<br/>`UInt64`          | `DecimalType(20,0)`             | `Decimal(20,0)`             | `Decimal(20,0)`            
| `UInt128`<br/>`UInt256`        | unsupported [^3]                |                             |                            


[^3]: Clickhouse support numeric types up to 256 bit - `Int256`, `UInt256`, `Decimal256(S)`, `Decimal(P=39..76, S=0..76)`.

    But Spark's `DecimalType(P, S)` supports maximum `P=38` (128 bit). It is impossible to read, write or operate with values of larger precision,
    this leads to an exception.

### Temporal types

Notes:
  - Datetime with timezone has the same precision as without timezone
  - `DateTime` is alias for `DateTime32`
  - `TIMESTAMP` is alias for `DateTime32`, but `TIMESTAMP(N)` is alias for `DateTime64(N)`


| Clickhouse type (read)            | Spark type                           | Clickhouse type (write)          | Clickhouse type (create)      |
|-----------------------------------|--------------------------------------|----------------------------------|-------------------------------|
| `Date`                          | `DateType()`                       | `Date`                         | `Date`                      |
| `Date32`                        | `DateType()`                       | `Date`                         | `Date`, **cannot insert data** [^4]                    |
|                                   |                                      |                                  | _   |
| `DateTime32`, seconds           | `TimestampType()`, microseconds    | `DateTime64(6)`, microseconds  | `DateTime32`, seconds       |
| `DateTime64(3)`, milliseconds   | `TimestampType()`, microseconds    | `DateTime64(6)`, microseconds  | `DateTime32`, seconds, **precision loss** [^5]     |
| `DateTime64(6)`, microseconds   | `TimestampType()`, microseconds    |                                  | `DateTime32`, seconds, **precision loss** [^7]      |
| `DateTime64(7..9)`, nanoseconds | `TimestampType()`, microseconds, **precision loss** [^6]   |                                  |                               |
| `-`                             | `TimestampNTZType()`, microseconds |                                  |                               |
| `DateTime32(TZ)`<br/>`DateTime64(P, TZ)`                | unsupported [^7]_                     |                                  |                               |
| `IntervalNanosecond`<br/>`IntervalMicrosecond`<br/>`IntervalMillisecond`<br/>`IntervalSecond`<br/>`IntervalMinute`<br/>`IntervalHour`<br/>`IntervalDay`<br/>`IntervalMonth`<br/>`IntervalQuarter`<br/>`IntervalWeek`<br/>`IntervalYear`            | <br/><br/><br/><br/><br/><br/>`LongType()`                       | <br/><br/><br/><br/><br/><br/>`Int64`                        |  <br/><br/><br/><br/><br/><br/>`Int64`                    |


!!! warning

    Note that types in Clickhouse and Spark have different value ranges:

    | Clickhouse type        | Min value                         | Max value                         | Spark type          | Min value                      | Max value                      | 
    |------------------------|-----------------------------------|-----------------------------------|---------------------|--------------------------------|--------------------------------| 
    | `Date`                 | `1970-01-01`                      | `2149-06-06`                      | <br/><br/>`DateType()` {: rowspan=3}        | <br/><br/>`0001-01-01 00:00:00.000000` {: rowspan=3}   | <br/><br/>`9999-12-31 23:59:59.999999` {:  rowspan=3} |   
    | `DateTime64(P=0..8)`   | `1900-01-01 00:00:00.00000000`    | `2299-12-31 23:59:59.99999999`  | &#8288 {: style="padding:0"} | &#8288 {: style="padding:0"} | &#8288 {: style="padding:0"} | 
    | `DateTime64(P=9)`      | `1900-01-01 00:00:00.000000000`   | `2262-04-11 23:47:16.999999999` | &#8288 {: style="padding:0"} | &#8288 {: style="padding:0"} | &#8288 {: style="padding:0"} | 
        
    So not all of values in Spark DataFrame can be written to Clickhouse.
        
    References:
         
    * [Clickhouse Date documentation](https://clickhouse.com/docs/en/sql-reference/data-types/date)
    * [Clickhouse Datetime32 documentation](https://clickhouse.com/docs/en/sql-reference/data-types/datetime)
    * [Clickhouse Datetime64 documentation](https://clickhouse.com/docs/en/sql-reference/data-types/datetime64)
    * [Spark DateType documentation](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html)
    * [Spark TimestampType documentation](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html)


[^4]: `Date32` has different bytes representation than `Date`, and inserting value of type `Date32` to `Date` column
    leads to errors on Clickhouse side, e.g. `Date(106617) should be between 0 and 65535 inclusive of both values`.
    Although Spark does properly read the `Date32` column as `DateType()`, and there should be no difference at all.
    Probably this is some bug in Clickhouse driver.

[^5]: Generic JDBC dialect generates DDL with Clickhouse type `TIMESTAMP` which is alias for `DateTime32` with precision up to seconds (`23:59:59`).
    Inserting data with milliseconds precision (`23:59:59.999`) will lead to **throwing away milliseconds**.
    Solution: create table manually, with proper column type.

[^6]: Clickhouse support datetime up to nanoseconds precision (`23:59:59.999999999`),
    but Spark `TimestampType()` supports datetime up to microseconds precision (`23:59:59.999999`).
    Nanoseconds will be lost during read or write operations.
    Solution: create table manually, with proper column type.

[^7]: Clickhouse will raise an exception that data in format `2001-01-01 23:59:59.999999` has data `.999999` which does not match format `YYYY-MM-DD hh:mm:ss`
    of `DateTime32` column type (see [^5]).
    So Spark can create Clickhouse table, but cannot write data to column of this type.
    Solution: create table manually, with proper column type.

### String types

| Clickhouse type (read)               | Spark type       | Clickhousetype (write) | Clickhouse type (create) |
|--------------------------------------|------------------|------------------------|--------------------------|
| `FixedString(N)`<br/>`String`<br/>`Enum8`<br/>`Enum16`<br/>`IPv4`<br/>`IPv6`<br/>`UUID`                   | <br/><br/><br/>`StringType()` | <br/><br/><br/>`String`             | <br/><br/><br/>`String`               |
| `-`                                | `BinaryType()` |                        |                          |

## Unsupported types

Columns of these Clickhouse types cannot be read by Spark:

  - `AggregateFunction(func, T)`
  - `Array(T)`
  - `JSON`
  - `Map(K, V)`
  - `MultiPolygon`
  - `Nested(field1 T1, ...)`
  - `Nothing`
  - `Point`
  - `Polygon`
  - `Ring`
  - `SimpleAggregateFunction(func, T)`
  - `Tuple(T1, T2, ...)`

Dataframe with these Spark types cannot be written to Clickhouse:

  - `ArrayType(T)`
  - `BinaryType()`
  - `CharType(N)`
  - `DayTimeIntervalType(P, S)`
  - `MapType(K, V)`
  - `NullType()`
  - `StructType([...])`
  - `TimestampNTZType()`
  - `VarcharType(N)`

This is because Spark does not have dedicated Clickhouse dialect, and uses Generic JDBC dialect instead.
This dialect does not have type conversion between some types, like Clickhouse `Array` -> Spark `ArrayType()`, and vice versa.

The is a way to avoid this - just cast everything to `String`.

## Explicit type cast

### `DBReader`

Use `CAST` or `toJSONString` to get column data as string in JSON format,

For parsing JSON columns in ClickHouse, [JSON.parse_column][onetl.file.format.json.JSON.parse_column] method.

```python
from pyspark.sql.types import ArrayType, IntegerType

from onetl.file.format import JSON
from onetl.connection import ClickHouse
from onetl.db import DBReader

reader = DBReader(
    connection=clickhouse,
    target="default.source_tbl",
    columns=[
        "id",
        "toJSONString(array_column) array_column",
    ],
)
df = reader.run()

# Spark requires all columns to have some specific type, describe it
column_type = ArrayType(IntegerType())

json = JSON()
df = df.select(
    df.id,
    json.parse_column("array_column", column_type),
)
```

### `DBWriter`

For writing JSON data to ClickHouse, use the [JSON.serialize_column][onetl.file.format.json.JSON.serialize_column] method to convert a DataFrame column to JSON format efficiently and write it as a `String` column in Clickhouse.

```python
from onetl.file.format import JSON
from onetl.connection import ClickHouse
from onetl.db import DBWriter

clickhouse = ClickHouse(...)

clickhouse.execute(
    """
    CREATE TABLE default.target_tbl (
        id Int32,
        array_column_json String,
    )
    ENGINE = MergeTree()
    ORDER BY id
    """,
)

json = JSON()
df = df.select(
    df.id,
    json.serialize_column(df.array_column).alias("array_column_json"),
)

writer.run(df)
```

Then you can parse this column on Clickhouse side - for example, by creating a view:

```sql
SELECT
    id,
    JSONExtract(json_column, 'Array(String)') AS array_column
FROM target_tbl
```

You can also use [ALIAS](https://clickhouse.com/docs/en/sql-reference/statements/create/table#alias)
or [MATERIALIZED](https://clickhouse.com/docs/en/sql-reference/statements/create/table#materialized) columns
to avoid writing such expression in every `SELECT` clause all the time:

```sql
CREATE TABLE default.target_tbl (
    id Int32,
    array_column_json String,
    -- computed column
    array_column Array(String) ALIAS JSONExtract(json_column, 'Array(String)')
    -- or materialized column
    -- array_column Array(String) MATERIALIZED JSONExtract(json_column, 'Array(String)')
)
ENGINE = MergeTree()
ORDER BY id
```

Downsides:

- Using `SELECT JSONExtract(...)` or `ALIAS` column can be expensive, because value is calculated on every row access. This can be especially harmful if such column is used in `WHERE` clause.
- `ALIAS` and `MATERIALIZED` columns are not included in `SELECT *` clause, they should be added explicitly: `SELECT *, calculated_column FROM table`.

!!! warning

    [EPHEMERAL](https://clickhouse.com/docs/en/sql-reference/statements/create/table#ephemeral) columns are not supported by Spark
    because they cannot be selected to determine target column type.
