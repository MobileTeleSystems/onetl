.. _clickhouse-types:

Clickhouse <-> Spark type mapping
=================================

Type detection & casting
------------------------

Spark's DataFrames always have a ``schema`` which is a list of columns with corresponding Spark types. All operations on a column are performed using column type.

Reading from Clickhouse
~~~~~~~~~~~~~~~~~~~~~~~

This is how Clickhouse connector performs this:

* For each column in query result (``SELECT column1, column2, ... FROM table ...``) get column name and Clickhouse type.
* Find corresponding ``Clickhouse type (read)`` -> ``Spark type`` combination (see below) for each DataFrame column. If no combination is found, raise exception.
* Create DataFrame from query with specific column names and Spark types.

Writing to some existing Clickhuse table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is how Clickhouse connector performs this:

* Get names of columns in DataFrame.
* Perform ``SELECT column1, colum2, ... FROM table LIMIT 0`` query
* For each column in query result get column name and Clickhouse type.
* **Find corresponding** ``Clickhouse type (read)`` -> ``Spark type`` **combination** (see below) for each DataFrame column. If no combination is found, raise exception. [1]_
* Find corresponding ``Spark type`` -> ``Clickhousetype (write)`` combination (see below) for each DataFrame column. If no combination is found, raise exception.
* If ``Clickhousetype (write)`` match ``Clickhouse type (read)``, no additional casts will be performed, DataFrame column will be written to Clickhouse as is.
* If ``Clickhousetype (write)`` does not match ``Clickhouse type (read)``, DataFrame column will be casted to target column type **on Clickhouse side**. For example, you can write column with text data to ``Int32`` column, if column contains valid integer values within supported value range and precision.

.. [1]

    Yes, this is weird.

Create new table using Spark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::

    ABSOLUTELY NOT RECOMMENDED!

This is how Clickhouse connector performs this:

* Find corresponding ``Spark type`` -> ``Clickhouse type (create)`` combination (see below) for each DataFrame column. If no combination is found, raise exception.
* Generate DDL for creating table in Clickhouse, like ``CREATE TABLE (col1 ...)``, and run it.
* Write DataFrame to created table as is.

But Spark does not have specific dialect for Clickhouse, so Generic JDBC dialect is used.
Generic dialect is using SQL ANSI type names while creating tables in target database, not database-specific types.

If some cases this may lead to using wrong column type. For example, Spark creates column of type ``TIMESTAMP``
which corresponds to Clickhouse's type ``DateTime32`` (precision up to seconds)
instead of more precise ``DateTime64`` (precision up to nanoseconds).
This may lead to incidental precision loss, or sometimes data cannot be written to created table at all.

So instead of relying on Spark to create tables:

.. code:: python

    writer = DBWriter(
        connection=clickhouse,
        table="default.target_tbl",
        options=Clickhouse.WriteOptions(
            if_exists="append",
            # ENGINE is required by Clickhouse
            createTableOptions="ENGINE = MergeTree() ORDER BY id",
        ),
    )
    writer.run(df)

Always prefer creating tables with specific types **BEFORE WRITING DATA**:

.. code:: python

    clickhouse.execute(
        """
        CREATE TABLE default.target_tbl AS (
            id UInt8,
            value DateTime64(6) -- specific type and precision
        )
        ENGINE = MergeTree()
        ORDER BY id
        """,
    )

    writer = DBWriter(
        connection=clickhouse,
        table="default.target_tbl",
        options=Clickhouse.WriteOptions(if_exists="append"),
    )
    writer.run(df)

References
~~~~~~~~~~

Here you can find source code with type conversions:

* `Clickhouse -> JDBC <https://github.com/ClickHouse/clickhouse-java/blob/0.3.2/clickhouse-jdbc/src/main/java/com/clickhouse/jdbc/JdbcTypeMapping.java#L39-L176>`_
* `JDBC -> Spark <https://github.com/apache/spark/blob/v3.5.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L307>`_
* `Spark -> JDBC <https://github.com/apache/spark/blob/v3.5.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L141-L164>`_
* `JDBC -> Clickhouse <https://github.com/ClickHouse/clickhouse-java/blob/0.3.2/clickhouse-jdbc/src/main/java/com/clickhouse/jdbc/JdbcTypeMapping.java#L185-L311>`_

Supported types
---------------

Generic types
~~~~~~~~~~~~~

* ``LowCardinality(T)`` is same as ``T``
* ``Nullable(T)`` is same as ``T``, but Spark column is inferred as ``nullable=True``

Numeric types
~~~~~~~~~~~~~

+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| Clickhouse type (read)         | Spark type                        | Clickhousetype (write)        | Clickhouse type (create)      |
+================================+===================================+===============================+===============================+
| ``Bool``                       | ``IntegerType()``                 | ``Int32``                     | ``Int32``                     |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``-``                          | ``BooleanType()``                 | ``UInt64``                    | ``UInt64``                    |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``Decimal``                    | ``DecimalType(P=10, S=0)``        | ``Decimal(P=10, S=0)``        | ``Decimal(P=10, S=0)``        |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``Decimal(P=0..38)``           | ``DecimalType(P=0..38, S=0)``     | ``Decimal(P=0..38, S=0)``     | ``Decimal(P=0..38, S=0)``     |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``Decimal(P=0..38, S=0..38)``  | ``DecimalType(P=0..38, S=0..38)`` | ``Decimal(P=0..38, S=0..38)`` | ``Decimal(P=0..38, S=0..38)`` |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``Decimal(P=39..76, S=0..76)`` | unsupported [2]_                  |                               |                               |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``Decimal32(P=0..9)``          | ``DecimalType(P=9, S=0..9)``      | ``Decimal(P=9, S=0..9)``      | ``Decimal(P=9, S=0..9)``      |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``Decimal64(S=0..18)``         | ``DecimalType(P=18, S=0..18)``    | ``Decimal(P=18, S=0..18)``    | ``Decimal(P=18, S=0..18)``    |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``Decimal128(S=0..38)``        | ``DecimalType(P=38, S=0..38)``    | ``Decimal(P=38, S=0..38)``    | ``Decimal(P=38, S=0..38)``    |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``Decimal256(S=0..76)``        | unsupported [2]_                  |                               |                               |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``Float32``                    | ``DoubleType()``                  | ``Float64``                   | ``Float64``                   |
+--------------------------------+                                   |                               |                               |
| ``Float64``                    |                                   |                               |                               |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``-``                          | ``FloatType()``                   | ``Float32``                   | ``Float32``                   |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``Int8``                       | ``IntegerType()``                 | ``Int32``                     | ``Int32``                     |
+--------------------------------+                                   |                               |                               |
| ``Int16``                      |                                   |                               |                               |
+--------------------------------+                                   |                               |                               |
| ``Int32``                      |                                   |                               |                               |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``Int64``                      | ``LongType()``                    | ``Int64``                     | ``Int64``                     |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``Int128``                     | ``DecimalType(20,0)``             | ``Decimal(20,0)``             | ``Decimal(20,0)``             |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``Int256``                     | unsupported [2]_                  |                               |                               |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``-``                          | ``ByteType()``                    | ``Int8``                      | ``Int8``                      |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``-``                          | ``ShortType()``                   | ``Int32``                     | ``Int32``                     |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``UInt8``                      | ``IntegerType()``                 | ``Int32``                     | ``Int32``                     |
+--------------------------------+                                   |                               |                               |
| ``UInt16``                     |                                   |                               |                               |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``UInt32``                     | ``DecimalType(20,0)``             | ``Decimal(20,0)``             | ``Decimal(20,0)``             |
+--------------------------------+                                   |                               |                               |
| ``UInt64``                     |                                   |                               |                               |
+--------------------------------+                                   |                               |                               |
| ``UInt128``                    |                                   |                               |                               |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+
| ``UInt256``                    | unsupported [2]_                  |                               |                               |
+--------------------------------+-----------------------------------+-------------------------------+-------------------------------+

.. [2]

    Clickhouse support numeric types up to 256 bit - ``Int256``, ``UInt256``, ``Decimal256(S)``, ``Decimal(P=39..76, S=0..76)``.

    But Spark's ``DecimalType(P, S)`` supports maximum ``P=38`` (128 bit). It is impossible to read, write or operate with values of larger precision,
    this leads to an exception.

Temporal types
~~~~~~~~~~~~~~

Note: ``DateTime(P, TZ)`` has the same precision as ``DateTime(P)``.

+-----------------------------------+--------------------------------------+----------------------------------+-------------------------------+
| Clickhouse type (read)            | Spark type                           | Clickhousetype (write)           | Clickhouse type (create)      |
+===================================+======================================+==================================+===============================+
| ``Date``                          | ``DateType()``                       | ``Date``                         | ``Date``                      |
+-----------------------------------+--------------------------------------+----------------------------------+-------------------------------+
| ``Date32``                        | unsupported                          |                                  |                               |
+-----------------------------------+--------------------------------------+----------------------------------+-------------------------------+
| ``DateTime32``, seconds           | ``TimestampType()``, microseconds    | ``DateTime64(6)``, microseconds  | ``DateTime32``, seconds,      |
+-----------------------------------+--------------------------------------+----------------------------------+-------------------------------+
| ``DateTime64(3)``, milliseconds   | ``TimestampType()``, microseconds    | ``DateTime64(6)``, microseconds  | ``DateTime32``, seconds,      |
|                                   |                                      |                                  | **precision loss** [4]_       |
|                                   |                                      |                                  |                               |
+-----------------------------------+--------------------------------------+----------------------------------+-------------------------------+
| ``DateTime64(6)``, microseconds   | ``TimestampType()``, microseconds    | ``DateTime64(6)``, microseconds  | ``DateTime32``, seconds,      |
+-----------------------------------+--------------------------------------+----------------------------------+ **cannot be inserted** [5]_   |
| ``DateTime64(7..9)``, nanoseconds | ``TimestampType()``, microseconds,   | ``DateTime64(6)``, microseconds, |                               |
|                                   | **precision loss** [3]_              | **precision loss** [3]_          |                               |
|                                   |                                      |                                  |                               |
+-----------------------------------+--------------------------------------+----------------------------------+                               |
| ``-``                             | ``TimestampNTZType()``, microseconds | ``DateTime64(6)``                |                               |
+-----------------------------------+--------------------------------------+----------------------------------+-------------------------------+
| ``IntervalNanosecond``            | unsupported                          |                                  |                               |
+-----------------------------------+                                      |                                  |                               |
| ``IntervalMicrosecond``           |                                      |                                  |                               |
+-----------------------------------+                                      |                                  |                               |
| ``IntervalMillisecond``           |                                      |                                  |                               |
+-----------------------------------+--------------------------------------+----------------------------------+-------------------------------+
| ``IntervalSecond``                | ``IntegerType()``                    | ``Int32``                        | ``Int32``                     |
+-----------------------------------+                                      |                                  |                               |
| ``IntervalMinute``                |                                      |                                  |                               |
+-----------------------------------+                                      |                                  |                               |
| ``IntervalHour``                  |                                      |                                  |                               |
+-----------------------------------+                                      |                                  |                               |
| ``IntervalDay``                   |                                      |                                  |                               |
+-----------------------------------+                                      |                                  |                               |
| ``IntervalMonth``                 |                                      |                                  |                               |
+-----------------------------------+                                      |                                  |                               |
| ``IntervalQuarter``               |                                      |                                  |                               |
+-----------------------------------+                                      |                                  |                               |
| ``IntervalWeek``                  |                                      |                                  |                               |
+-----------------------------------+                                      |                                  |                               |
| ``IntervalYear``                  |                                      |                                  |                               |
+-----------------------------------+--------------------------------------+----------------------------------+-------------------------------+

.. [3]
    Clickhouse support datetime up to nanoseconds precision (``23:59:59.999999999``),
    but Spark ``TimestampType()`` supports datetime up to microseconds precision (``23:59:59.999999``).
    Nanoseconds will be lost during read or write operations.

.. [4]
    Generic JDBC dialect generates DDL with Clickhouse type ``TIMESTAMP`` which is alias for ``DateTime32`` with precision up to seconds (``23:59:59``).
    Inserting data with milliseconds precision (``23:59:59.999``) will lead to throwing away milliseconds (``23:59:59``).

.. [5]
    Clickhouse will raise an exception that data in format ``2001-01-01 23:59:59.999999`` has data ``.999999`` which does not match format ``YYYY-MM-DD hh:mm:ss``.
    So you can create Clickhouse table with Spark, but cannot write data to column of this type.

String types
~~~~~~~~~~~~~

+--------------------------------------+------------------+------------------------+--------------------------+
| Clickhouse type (read)               | Spark type       | Clickhousetype (write) | Clickhouse type (create) |
+======================================+==================+========================+==========================+
| ``IPv4``                             | ``StringType()`` | ``String``             | ``String``               |
+--------------------------------------+                  |                        |                          |
| ``IPv6``                             |                  |                        |                          |
+--------------------------------------+                  |                        |                          |
| ``Enum8``                            |                  |                        |                          |
+--------------------------------------+                  |                        |                          |
| ``Enum16``                           |                  |                        |                          |
+--------------------------------------+                  |                        |                          |
| ``FixedString(N)``                   |                  |                        |                          |
+--------------------------------------+                  |                        |                          |
| ``String``                           |                  |                        |                          |
+--------------------------------------+------------------+                        |                          |
| ``-``                                | ``BinaryType()`` |                        |                          |
+--------------------------------------+------------------+------------------------+--------------------------+

Unsupported types
-----------------

Columns of these Clickhouse types cannot be read by Spark:
    * ``AggregateFunction(func, T)``
    * ``Array(T)``
    * ``JSON``
    * ``Map(K, V)``
    * ``MultiPolygon``
    * ``Nested(field1 T1, ...)``
    * ``Nothing``
    * ``Point``
    * ``Polygon``
    * ``Ring``
    * ``SimpleAggregateFunction(func, T)``
    * ``Tuple(T1, T2, ...)``
    * ``UUID``

Dataframe with these Spark types be written to Clickhouse:
    * ``ArrayType(T)``
    * ``BinaryType()``
    * ``CharType(N)``
    * ``DayTimeIntervalType(P, S)``
    * ``MapType(K, V)``
    * ``NullType()``
    * ``StructType([...])``
    * ``TimestampNTZType()``
    * ``VarcharType(N)``

This is because Spark does not have dedicated Clickhouse dialect, and uses Generic JDBC dialect instead.
This dialect does not have type conversion between some types, like Clickhouse ``Array`` -> Spark ``ArrayType()``, and vice versa.

The is a way to avoid this - just cast everything to ``String``.

Read unsupported column type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``CAST`` or ``toJSONString`` to get column data as string in JSON format,
and then cast string column in resulting dataframe to proper type using `from_json <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html>`_:

.. code:: python

    from pyspark.sql.functions import from_json
    from pyspark.sql.types import ArrayType, IntegerType

    reader = DBReader(
        connection=clickhouse,
        columns=[
            "id",
            "toJSONString(array_column) array_column",
        ],
    )
    df = reader.run()

    # Spark requires all columns to have some specific type, describe it
    column_type = ArrayType(IntegerType())

    df = df.select(
        df.id,
        from_json(df.array_column, column_type).alias("array_column"),
    )

Write unsupported column type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Convert dataframe column to JSON using `to_json <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html>`_,
and write it as ``String`` column in Clickhouse:

.. code:: python

    clickhouse.execute(
        """
        CREATE TABLE target_tbl AS (
            id Int32,
            array_column_json String,
        )
        ENGINE = MergeTree()
        ORDER BY time
        """,
    )

    from pyspark.sql.functions import to_json

    df = df.select(
        df.id,
        to_json(df.array_column).alias("array_column_json"),
    )

    writer.run(df)

Then you can parse this column on Clickhouse side:

.. code:: sql

    SELECT id, JSONExtract(json_column, 'Array(String)') FROM target_tbl

You can also use `MATERIALIZED <https://clickhouse.com/docs/en/sql-reference/statements/create/table#materialized>`_
and `ALIAS <https://clickhouse.com/docs/en/sql-reference/statements/create/table#alias>`_ columns
to avoid writing such expression in every ``SELECT`` clause all the time.

Downsides:

* Using ``SELECT JSONExtract(...)`` or ``ALIAS`` column can be expensive, because value is calculated on every row access. This can be especially harmful if such column is used in ``WHERE`` clause.
* Using ``MATERIALIZED`` column allows to perform such expensive calculation just once, but this requires up to 2x storage, because Clickhouse stores both raw and parsed column.
* Both ``ALIAS`` and ``MATERIALIZED`` columns are not included in ``SELECT *`` clause, they should be added explicitly: ``SELECT *, calculated_column FROM table``.

.. warning::

    `EPHEMERAL <https://clickhouse.com/docs/en/sql-reference/statements/create/table#ephemeral>`_ columns are not supported by Spark
    because they cannot be selected to determine target column type.