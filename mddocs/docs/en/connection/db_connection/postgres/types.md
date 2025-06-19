(postgres-types)=

# Postgres \<-> Spark type mapping

```{eval-rst}
.. note::

    The results below are valid for Spark 3.5.5, and may differ on other Spark versions.
```

## Type detection & casting

Spark's DataFrames always have a `schema` which is a list of columns with corresponding Spark types. All operations on a column are performed using column type.

### Reading from Postgres

This is how Postgres connector performs this:

- For each column in query result (`SELECT column1, column2, ... FROM table ...`) get column name and Postgres type.
- Find corresponding `Postgres type (read)` → `Spark type` combination (see below) for each DataFrame column [^footnote-1]. If no combination is found, raise exception.
- Create DataFrame from query with specific column names and Spark types.

[^footnote-1]: All Postgres types that doesn't have corresponding Java type are converted to `String`.

### Writing to some existing Postgres table

This is how Postgres connector performs this:

- Get names of columns in DataFrame. [^footnote-1]
- Perform `SELECT * FROM table LIMIT 0` query.
- Take only columns present in DataFrame (by name, case insensitive) [^footnote-2]. For each found column get Postgres type.
- Find corresponding `Spark type` → `Postgres type (write)` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- If `Postgres type (write)` match `Postgres type (read)`, no additional casts will be performed, DataFrame column will be written to Postgres as is.
- If `Postgres type (write)` does not match `Postgres type (read)`, DataFrame column will be casted to target column type **on Postgres side**.
  For example, you can write column with text data to `int` column, if column contains valid integer values within supported value range and precision [^footnote-3].

[^footnote-2]: This allows to write data to tables with `DEFAULT` and `GENERATED` columns - if DataFrame has no such column,
    it will be populated by Postgres.

[^footnote-3]: This is true only if either DataFrame column is a `StringType()`, or target column is `text` type.

    But other types cannot be silently converted, like `bytea -> bit(N)`. This requires explicit casting, see [Manual conversion to string].

### Create new table using Spark

```{eval-rst}
.. warning::

    ABSOLUTELY NOT RECOMMENDED!
```

This is how Postgres connector performs this:

- Find corresponding `Spark type` → `Postgres type (create)` combination (see below) for each DataFrame column. If no combination is found, raise exception.
- Generate DDL for creating table in Postgres, like `CREATE TABLE (col1 ...)`, and run it.
- Write DataFrame to created table as is.

But Postgres connector support only limited number of types and almost no custom clauses (like `PARTITION BY`, `INDEX`, etc).
So instead of relying on Spark to create tables:

```{eval-rst}
.. dropdown:: See example

    .. code:: python

        writer = DBWriter(
            connection=postgres,
            target="public.table",
            options=Postgres.WriteOptions(
                if_exists="append",
                createTableOptions="PARTITION BY RANGE (id)",
            ),
        )
        writer.run(df)
```

Always prefer creating table with desired DDL **BEFORE WRITING DATA**:

```{eval-rst}
.. dropdown:: See example

    .. code:: python

        postgres.execute(
            """
            CREATE TABLE public.table (
                id bigint,
                business_dt timestamp(6),
                value json
            )
            PARTITION BY RANGE (Id)
            """,
        )

        writer = DBWriter(
            connection=postgres,
            target="public.table",
            options=Postgres.WriteOptions(if_exists="append"),
        )
        writer.run(df)
```

See Postgres [CREATE TABLE](https://www.postgresql.org/docs/current/sql-createtable.html) documentation.

## Supported types

### References

See [List of Postgres types](https://www.postgresql.org/docs/current/datatype.html).

Here you can find source code with type conversions:

- [Postgres \<-> JDBC](https://github.com/pgjdbc/pgjdbc/blob/REL42.6.0/pgjdbc/src/main/java/org/postgresql/jdbc/TypeInfoCache.java#L78-L112)
- [JDBC -> Spark](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/PostgresDialect.scala#L52-L108)
- [Spark -> JDBC](https://github.com/apache/spark/blob/v3.5.5/sql/core/src/main/scala/org/apache/spark/sql/jdbc/PostgresDialect.scala#L118-L132)

### Numeric types

```{eval-rst}
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| Postgres type (read)             | Spark type                        | Postgres type (write)         | Postgres type (create)  |
+==================================+===================================+===============================+=========================+
| ``decimal``                      | ``DecimalType(P=38, S=18)``       | ``decimal(P=38, S=18)``       | ``decimal`` (unbounded) |
+----------------------------------+-----------------------------------+-------------------------------+                         |
| ``decimal(P=0..38)``             | ``DecimalType(P=0..38, S=0)``     | ``decimal(P=0..38, S=0)``     |                         |
+----------------------------------+-----------------------------------+-------------------------------+                         |
| ``decimal(P=0..38, S=0..38)``    | ``DecimalType(P=0..38, S=0..38)`` | ``decimal(P=0..38, S=0..38)`` |                         |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``decimal(P=39.., S=0..)``       | unsupported [4]_                  |                               |                         |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``decimal(P=.., S=..-1)``        | unsupported [5]_                  |                               |                         |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``real``                         | ``FloatType()``                   | ``real``                      | ``real``                |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``double precision``             | ``DoubleType()``                  | ``double precision``          | ``double precision``    |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``smallint``                     | ``ShortType()``                   | ``smallint``                  | ``smallint``            |
+----------------------------------+-----------------------------------+                               |                         |
| ``-``                            | ``ByteType()``                    |                               |                         |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``integer``                      | ``IntegerType()``                 | ``integer``                   | ``integer``             |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``bigint``                       | ``LongType()``                    | ``bigint``                    | ``bigint``              |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``money``                        | ``StringType()`` [1]_             | ``text``                      | ``text``                |
+----------------------------------+                                   |                               |                         |
| ``int4range``                    |                                   |                               |                         |
+----------------------------------+                                   |                               |                         |
| ``int8range``                    |                                   |                               |                         |
+----------------------------------+                                   |                               |                         |
| ``numrange``                     |                                   |                               |                         |
+----------------------------------+                                   |                               |                         |
| ``int2vector``                   |                                   |                               |                         |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
```

[^footnote-4]: Postgres support decimal types with unlimited precision.

    But Spark's `DecimalType(P, S)` supports maximum `P=38` (128 bit). It is impossible to read, write or operate with values of larger precision,
    this leads to an exception.

[^footnote-5]: Postgres support decimal types with negative scale, like `decimal(38, -10)`. Spark doesn't.

### Temporal types

```{eval-rst}
+------------------------------------+------------------------------+-----------------------+-------------------------+
| Postgres type (read)               | Spark type                   | Postgres type (write) | Postgres type (create)  |
+====================================+==============================+=======================+=========================+
| ``date``                           | ``DateType()``               | ``date``              | ``date``                |
+------------------------------------+------------------------------+-----------------------+-------------------------+
| ``time``                           | ``TimestampType()``,         | ``timestamp(6)``      | ``timestamp(6)``        |
+------------------------------------+ with time format quirks [6]_ |                       |                         |
| ``time(0..6)``                     |                              |                       |                         |
+------------------------------------+                              |                       |                         |
| ``time with time zone``            |                              |                       |                         |
+------------------------------------+                              |                       |                         |
| ``time(0..6) with time zone``      |                              |                       |                         |
+------------------------------------+------------------------------+-----------------------+-------------------------+
| ``timestamp``                      | ``TimestampType()``          | ``timestamp(6)``      | ``timestamp(6)``        |
+------------------------------------+                              |                       |                         |
| ``timestamp(0..6)``                |                              |                       |                         |
+------------------------------------+                              |                       |                         |
| ``timestamp with time zone``       |                              |                       |                         |
+------------------------------------+                              |                       |                         |
| ``timestamp(0..6) with time zone`` |                              |                       |                         |
+------------------------------------+------------------------------+-----------------------+-------------------------+
| ``-``                              | ``TimestampNTZType()``       | ``timestamp(6)``      | ``timestamp(6)``        |
+------------------------------------+------------------------------+-----------------------+-------------------------+
| ``interval`` of any precision      | ``StringType()`` [1]_        | ``text``              | ``text``                |
+------------------------------------+------------------------------+-----------------------+-------------------------+
| ``-``                              | ``DayTimeIntervalType()``    | unsupported           | unsupported             |
+------------------------------------+------------------------------+-----------------------+-------------------------+
| ``-``                              | ``YearMonthIntervalType()``  | unsupported           | unsupported             |
+------------------------------------+------------------------------+-----------------------+-------------------------+
| ``daterange``                      | ``StringType()`` [1]_        | ``text``              | ``text``                |
+------------------------------------+                              |                       |                         |
| ``tsrange``                        |                              |                       |                         |
+------------------------------------+                              |                       |                         |
| ``tstzrange``                      |                              |                       |                         |
+------------------------------------+------------------------------+-----------------------+-------------------------+
```

```{eval-rst}
.. warning::

    Note that types in Postgres and Spark have different value ranges:

    +---------------+---------------------------------+----------------------------------+---------------------+--------------------------------+--------------------------------+
    | Postgres type | Min value                       | Max value                        | Spark type          | Min value                      | Max value                      |
    +===============+=================================+==================================+=====================+================================+================================+
    | ``date``      | ``-4713-01-01``                 | ``5874897-01-01``                | ``DateType()``      | ``0001-01-01``                 | ``9999-12-31``                 |
    +---------------+---------------------------------+----------------------------------+---------------------+--------------------------------+--------------------------------+
    | ``timestamp`` | ``-4713-01-01 00:00:00.000000`` | ``294276-12-31 23:59:59.999999`` | ``TimestampType()`` | ``0001-01-01 00:00:00.000000`` | ``9999-12-31 23:59:59.999999`` |
    +---------------+---------------------------------+----------------------------------+                     |                                |                                |
    | ``time``      | ``00:00:00.000000``             | ``24:00:00.000000``              |                     |                                |                                |
    +---------------+---------------------------------+----------------------------------+---------------------+--------------------------------+--------------------------------+

    So not all of values can be read from Postgres to Spark.

    References:
        * `Postgres date/time types documentation <https://www.postgresql.org/docs/current/datatype-datetime.html>`_
        * `Spark DateType documentation <https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html>`_
        * `Spark TimestampType documentation <https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html>`_
```

[^footnote-6]: `time` type is the same as `timestamp` with date `1970-01-01`. So instead of reading data from Postgres like `23:59:59`
    it is actually read `1970-01-01 23:59:59`, and vice versa.

### String types

```{eval-rst}
+-----------------------------+-----------------------+-----------------------+-------------------------+
| Postgres type (read)        | Spark type            | Postgres type (write) | Postgres type (create)  |
+=============================+=======================+=======================+=========================+
| ``character``               | ``StringType()``      | ``text``              | ``text``                |
+-----------------------------+                       |                       |                         |
| ``character(N)``            |                       |                       |                         |
+-----------------------------+                       |                       |                         |
| ``character varying``       |                       |                       |                         |
+-----------------------------+                       |                       |                         |
| ``character varying(N)``    |                       |                       |                         |
+-----------------------------+                       |                       |                         |
| ``text``                    |                       |                       |                         |
+-----------------------------+                       |                       |                         |
| ``json``                    |                       |                       |                         |
+-----------------------------+                       |                       |                         |
| ``jsonb``                   |                       |                       |                         |
+-----------------------------+                       |                       |                         |
| ``xml``                     |                       |                       |                         |
+-----------------------------+-----------------------+                       |                         |
| ``CREATE TYPE ... AS ENUM`` | ``StringType()`` [1]_ |                       |                         |
+-----------------------------+                       |                       |                         |
| ``tsvector``                |                       |                       |                         |
+-----------------------------+                       |                       |                         |
| ``tsquery``                 |                       |                       |                         |
+-----------------------------+-----------------------+-----------------------+-------------------------+
| ``-``                       | ``CharType()``        | ``unsupported``       | ``unsupported``         |
+-----------------------------+-----------------------+-----------------------+-------------------------+
| ``-``                       | ``VarcharType()``     | ``unsupported``       | ``unsupported``         |
+-----------------------------+-----------------------+-----------------------+-------------------------+
```

### Binary types

```{eval-rst}
+--------------------------+-----------------------+-----------------------------+-------------------------+
| Postgres type (read)     | Spark type            | Postgres type (write)       | Postgres type (create)  |
+==========================+=======================+=============================+=========================+
| ``boolean``              | ``BooleanType()``     | ``boolean``                 | ``boolean``             |
+--------------------------+-----------------------+-----------------------------+-------------------------+
| ``bit``                  | ``BooleanType()``     | ``bool``,                   | ``bool``                |
+--------------------------+                       | **cannot insert data** [3]_ |                         |
| ``bit(N=1)``             |                       |                             |                         |
+--------------------------+-----------------------+-----------------------------+-------------------------+
| ``bit(N=2..)``           | ``ByteType()``        | ``bytea``,                  | ``bytea``               |
|                          |                       | **cannot insert data** [3]_ |                         |
+--------------------------+-----------------------+-----------------------------+-------------------------+
| ``bit varying``          | ``StringType()`` [1]_ | ``text``                    | ``text``                |
+--------------------------+                       |                             |                         |
| ``bit varying(N)``       |                       |                             |                         |
+--------------------------+-----------------------+-----------------------------+-------------------------+
| ``bytea``                | ``BinaryType()``      | ``bytea``                   | ``bytea``               |
+--------------------------+-----------------------+-----------------------------+-------------------------+
```

### Struct types

```{eval-rst}
+--------------------------------+-----------------------+-----------------------+-------------------------+
| Postgres type (read)           | Spark type            | Postgres type (write) | Postgres type (create)  |
+================================+=======================+=======================+=========================+
| ``T[]``                        | ``ArrayType(T)``      | ``T[]``               | ``T[]``                 |
+--------------------------------+-----------------------+-----------------------+-------------------------+
| ``T[][]``                      | unsupported           |                       |                         |
+--------------------------------+-----------------------+-----------------------+-------------------------+
| ``CREATE TYPE sometype (...)`` | ``StringType()`` [1]_ | ``text``              | ``text``                |
+--------------------------------+-----------------------+-----------------------+-------------------------+
| ``-``                          | ``StructType()``      | unsupported           |                         |
+--------------------------------+-----------------------+                       |                         |
| ``-``                          | ``MapType()``         |                       |                         |
+--------------------------------+-----------------------+-----------------------+-------------------------+
```

### Network types

```{eval-rst}
+----------------------+-----------------------+-----------------------+-------------------------+
| Postgres type (read) | Spark type            | Postgres type (write) | Postgres type (create)  |
+======================+=======================+=======================+=========================+
| ``cidr``             | ``StringType()`` [1]_ | ``text``              | ``text``                |
+----------------------+                       |                       |                         |
| ``inet``             |                       |                       |                         |
+----------------------+                       |                       |                         |
| ``macaddr``          |                       |                       |                         |
+----------------------+                       |                       |                         |
| ``macaddr8``         |                       |                       |                         |
+----------------------+-----------------------+-----------------------+-------------------------+
```

### Geo types

```{eval-rst}
+----------------------+-----------------------+-----------------------+-------------------------+
| Postgres type (read) | Spark type            | Postgres type (write) | Postgres type (create)  |
+======================+=======================+=======================+=========================+
| ``circle``           | ``StringType()`` [1]_ | ``text``              | ``text``                |
+----------------------+                       |                       |                         |
| ``box``              |                       |                       |                         |
+----------------------+                       |                       |                         |
| ``line``             |                       |                       |                         |
+----------------------+                       |                       |                         |
| ``lseg``             |                       |                       |                         |
+----------------------+                       |                       |                         |
| ``path``             |                       |                       |                         |
+----------------------+                       |                       |                         |
| ``point``            |                       |                       |                         |
+----------------------+                       |                       |                         |
| ``polygon``          |                       |                       |                         |
+----------------------+                       |                       |                         |
| ``polygon``          |                       |                       |                         |
+----------------------+-----------------------+-----------------------+-------------------------+
```

## Explicit type cast

### `DBReader`

It is possible to explicitly cast column of unsupported type using `DBReader(columns=...)` syntax.

For example, you can use `CAST(column AS text)` to convert data to string representation on Postgres side, and so it will be read as Spark's `StringType()`.

It is also possible to use [to_json](https://www.postgresql.org/docs/current/functions-json.html) Postgres function to convert column of any type to string representation, and then parse this column on Spark side you can use the {obj}`JSON.parse_column <onetl.file.format.json.JSON.parse_column>` method:

```python
from pyspark.sql.types import IntegerType

from onetl.connection import Postgres
from onetl.db import DBReader
from onetl.file.format import JSON

postgres = Postgres(...)

DBReader(
    connection=postgres,
    columns=[
        "id",
        "supported_column",
        "CAST(unsupported_column AS text) unsupported_column_str",
        # or
        "to_json(unsupported_column) array_column_json",
    ],
)
df = reader.run()

json_schema = StructType(
    [
        StructField("id", IntegerType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        ...,
    ]
)
df = df.select(
    df.id,
    df.supported_column,
    # explicit cast
    df.unsupported_column_str.cast("integer").alias("parsed_integer"),
    JSON().parse_column("array_column_json", json_schema).alias("json_string"),
)
```

### `DBWriter`

It is always possible to convert data on the Spark side to a string, and then write it to a text column in a Postgres table.

#### Using JSON.serialize_column

You can use the {obj}`JSON.serialize_column <onetl.file.format.json.JSON.serialize_column>` method for data serialization:

```python
from onetl.file.format import JSON
from pyspark.sql.functions import col

from onetl.connection import Postgres
from onetl.db import DBWriter

postgres = Postgres(...)

postgres.execute(
    """
    CREATE TABLE schema.target_table (
        id int,
        supported_column timestamp,
        array_column_json jsonb -- any column type, actually
    )
    """,
)

write_df = df.select(
    df.id,
    df.supported_column,
    JSON().serialize_column(df.unsupported_column).alias("array_column_json"),
)

writer = DBWriter(
    connection=postgres,
    target="schema.target_table",
)
writer.run(write_df)
```

Then you can parse this column on the Postgres side (for example, by creating a view):

```sql
SELECT
    id,
    supported_column,
    array_column_json->'0' AS array_item_0
FROM
    schema.target_table
```

To avoid casting the value on every table read you can use [GENERATED ALWAYS STORED](https://www.postgresql.org/docs/current/ddl-generated-columns.html) column, but this requires 2x space (for original and parsed value).

#### Manual conversion to string

Postgres connector also supports conversion text value directly to target column type, if this value has a proper format.

For example, you can write data like `[123, 345)` to `int8range` type because Postgres allows cast `'[123, 345)'::int8range'`:

```python
from pyspark.sql.ftypes import StringType
from pyspark.sql.functions import udf

from onetl.connection import Postgres
from onetl.db import DBReader

postgres = Postgres(...)

postgres.execute(
    """
    CREATE TABLE schema.target_table (
        id int,
        range_column int8range -- any column type, actually
    )
    """,
)


@udf(returnType=StringType())
def array_to_range(value: tuple):
    """This UDF allows to convert tuple[start, end] to Postgres' range format"""
    start, end = value
    return f"[{start},{end})"


write_df = df.select(
    df.id,
    array_to_range(df.range_column).alias("range_column"),
)

writer = DBWriter(
    connection=postgres,
    target="schema.target_table",
)
writer.run(write_df)
```

This can be tricky to implement and may lead to longer write process.
But this does not require extra space on Postgres side, and allows to avoid explicit value cast on every table read.
