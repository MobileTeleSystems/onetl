.. _clickhouse-types:

Clickhouse <-> Spark type mapping
=================================

Generic types
-------------

* ``LowCardinality(T)`` is same as ``T``
* ``Nullable(T)`` is same as ``T``, but Spark column is inferred as ``nullable=True``

Numeric types
-------------

+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| Clickhouse type (read)               | Spark type                                 | Clickhousetype (write)                 | Clickhouse type (create table using Spark) |
+======================================+============================================+========================================+============================================+
| ``Bool``                             | ``IntegerType()``                          | ``Int32``                              | ``Int32``                                  |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``Decimal``                          | ``DecimalType(10,0)``                      | ``Decimal(10,0)``                      | ``Decimal(10,0)``                          |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``Decimal(P)``                       | ``DecimalType(P,0)``                       | ``Decimal(P,0)``                       | ``Decimal(P,0)``                           |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``Decimal(P,S)``, 0 <= P <= S <= 38  | ``DecimalType(P,S)``                       | ``Decimal(P,S)``                       | ``Decimal(P,S)``                           |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``Decimal(P,S)``, P,S > 38           | ``DecimalType(38,38)``, **precision loss** | ``Decimal(38,38)``, **precision loss** | ``Decimal(38,38)``, **precision loss**     |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``Decimal32(S)``                     | ``DecimalType(9,S)``                       | ``Decimal(9,S)``                       | ``Decimal(9,S)``                           |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``Decimal64(S)``                     | ``DecimalType(18,S)``                      | ``Decimal(18,S)``                      | ``Decimal(18,S)``                          |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``Decimal128(S)``                    | ``DecimalType(38,S)``                      | ``Decimal(38,S)``                      | ``Decimal(38,S)``                          |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``Decimal256(S)``                    | ``DecimalType(38,S)``, **precision loss**  | ``Decimal(38,S)``, **precision loss**  | ``Decimal(38,S)``, **precision loss**      |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``Float32``                          | ``DoubleType()``                           | ``Float64``                            | ``Float64``                                |
+--------------------------------------+                                            +                                        +                                            +
| ``Float64``                          |                                            |                                        |                                            |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``Int8``                             | ``IntegerType()``                          | ``Int32``                              | ``INTEGER``                                |
+--------------------------------------+                                            +                                        +                                            +
| ``Int16``                            |                                            |                                        |                                            |
+--------------------------------------+                                            +                                        +                                            +
| ``Int32``                            |                                            |                                        |                                            |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``Int64``                            | ``LongType()``                             | ``Int64``                              | ``BIGINT``                                 |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``Int128``                           | ``DecimalType(20,0)``                      | ``Decimal(20,0)``                      | ``Decimal(20,0)``                          |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``Int256``                           | ``DecimalType(38,0)``, **precision loss**  | ``Decimal(38,0)``, **precision loss**  | ``Decimal(38,0)``, **precision loss**      |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``UInt8``                            | ``IntegerType()``                          | ``Int32``                              | ``INTEGER``                                |
+--------------------------------------+                                            +                                        +                                            +
| ``UInt16``                           |                                            |                                        |                                            |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``UInt32``                           | ``DecimalType(20,0)``                      | ``Decimal(20,0)``                      | ``Decimal(20,0)``                          |
+--------------------------------------+                                            +                                        +                                            +
| ``UInt64``                           |                                            |                                        |                                            |
+--------------------------------------+                                            +                                        +                                            +
| ``UInt128``                          |                                            |                                        |                                            |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+
| ``UInt256``                          | ``DecimalType(38,0)``, **precision loss**  | ``Decimal(38,0)``, **precision loss**  | ``Decimal(38,0)``, **precision loss**      |
+--------------------------------------+--------------------------------------------+----------------------------------------+--------------------------------------------+

Temporal types
--------------

+--------------------------------------+--------------------------------------------+----------------------------------+--------------------------------------------+
| Clickhouse type (read)               | Spark type                                 | Clickhousetype (write)           | Clickhouse type (create table using Spark) |
+======================================+============================================+==================================+============================================+
| ``Date``                             | ``DateType()``                             | ``Date``                         | ``Date``                                   |
+--------------------------------------+--------------------------------------------+----------------------------------+--------------------------------------------+
| ``Date32``                           | unsupported                                |                                  |                                            |
+--------------------------------------+--------------------------------------------+----------------------------------+--------------------------------------------+
| ``DateTime32``, seconds              | ``TimestampType()``, milliseconds          | ``DateTime64(6)``, milliseconds, | ``DateTime``, seconds, **precision loss**  |
+--------------------------------------+                                            +                                  +                                            +
| ``DateTime64(3)``, milliseconds      |                                            |                                  |                                            |
+--------------------------------------+                                            +                                  +                                            +
| ``DateTime64(6)``, microseconds      |                                            |                                  |                                            |
+--------------------------------------+--------------------------------------------+----------------------------------+                                            +
| ``DateTime64(7..9)``, nanoseconds    | ``TimestampType()``, milliseconds,         | ``DateTime64(6)``, milliseconds, |                                            |
|                                      | **precision loss**                         | **precision loss**               |                                            +
+--------------------------------------+--------------------------------------------+----------------------------------+--------------------------------------------+
| ``IntervalNanosecond``               | unsupported                                |                                  |                                            |
+--------------------------------------+                                            +                                  +                                            +
| ``IntervalMicrosecond``              |                                            |                                  |                                            |
+--------------------------------------+                                            +                                  +                                            +
| ``IntervalMillisecond``              |                                            |                                  |                                            |
+--------------------------------------+--------------------------------------------+----------------------------------+--------------------------------------------+
| ``IntervalSecond``                   | ``IntegerType()``                          | ``Int32``                        | ``Int32``                                  |
+--------------------------------------+                                            +                                  +                                            +
| ``IntervalMinute``                   |                                            |                                  |                                            |
+--------------------------------------+                                            +                                  +                                            +
| ``IntervalHour``                     |                                            |                                  |                                            |
+--------------------------------------+                                            +                                  +                                            +
| ``IntervalDay``                      |                                            |                                  |                                            |
+--------------------------------------+                                            +                                  +                                            +
| ``IntervalMonth``                    |                                            |                                  |                                            |
+--------------------------------------+                                            +                                  +                                            +
| ``IntervalQuarter``                  |                                            |                                  |                                            |
+--------------------------------------+                                            +                                  +                                            +
| ``IntervalWeek``                     |                                            |                                  |                                            |
+--------------------------------------+                                            +                                  +                                            +
| ``IntervalYear``                     |                                            |                                  |                                            |
+--------------------------------------+--------------------------------------------+----------------------------------+--------------------------------------------+

String types
------------

+--------------------------------------+------------------+------------------------+--------------------------------------------+
| Clickhouse type (read)               | Spark type       | Clickhousetype (write) | Clickhouse type (create table using Spark) |
+======================================+==================+========================+============================================+
| ``IPv4``                             | ``StringType()`` | ``String``             | ``String``                                 |
+--------------------------------------+                  +                        +                                            +
| ``IPv6``                             |                  |                        |                                            |
+--------------------------------------+                  +                        +                                            +
| ``Enum8``                            |                  |                        |                                            |
+--------------------------------------+                  +                        +                                            +
| ``Enum16``                           |                  |                        |                                            |
+--------------------------------------+                  +                        +                                            +
| ``FixedString(N)``                   |                  |                        |                                            |
+--------------------------------------+                  +                        +                                            +
| ``String``                           |                  |                        |                                            |
+--------------------------------------+------------------+------------------------+--------------------------------------------+

Unsupported types
-----------------

Columns of these types cannot be read/written by Spark:
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

This is because there is no dedicated Clickhouse dialect for Spark, so some types cannot be properly converted to Spark types (e.g. ``Array``),
even if Spark does support such type (e.g. ``ArrayType()``).

The is a way to avoid this - just cast unsupported type to ``String``.

Read unsupported column type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``CAST`` or ``toJSONString`` to get column data as string in JSON format:

.. code:: sql

    SELECT CAST(array_column AS String) FROM ...

    -- or

    SELECT toJSONString(array_column) FROM ...

And then cast string column in resulting dataframe to proper type using `from_json <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html>`_:

.. code:: python

    from pyspark.sql.functions import from_json
    from pyspark.sql.types import ArrayType, IntegerType

    df = clickhouse.sql(...)

    # Spark requires all columns to have some specific type, describe it
    column_type = ArrayType(IntegerType())

    parsed_array = from_json(df.array_column, schema).alias("array_column")
    df = df.select(parsed_array)

Write unsupported column type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Convert dataframe column to JSON using `to_json <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html>`_,
and write it as ``String`` column in Clickhouse:

.. code:: sql

    CREATE TABLE target_tbl AS (
        id Int32,
        array_column_json String,
    )
    ENGINE = MergeTree()
    ORDER BY time

.. code:: python

    from pyspark.sql.functions import to_json

    array_column_json = to_json(df.array_column)
    df = df.select(df.id, array_column_json.alias("array_column_json"))

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

Creating tables using Spark
---------------------------

.. warning::

    Absolutely not recommended!

If Spark dataframe is written to Clickhouse table which does not exists yet, it will be automatically created.

But Spark will use types from Generic JDBC dialect, and generic types like ``TIMESTAMP`` have different precision
than Clickhouse-specific ``DateTime32`` / ``DateTime64``.

Always prefer creating tables with specific types **BEFORE WRITING DATA** using Spark:

.. code:: python

    clickhouse.execute(
        """
        CREATE TABLE target_tbl AS (
            id UInt8,
            value DateTime64(6) -- specific type and precision
        )
        ENGINE = MergeTree()
        ORDER BY value
        """,
    )

References
----------

Here you can find the source code used by Clickhouse JDBC and Spark for performing type conversion:

* `Clickhouse -> JDBC <https://github.com/ClickHouse/clickhouse-java/blob/0.3.2/clickhouse-jdbc/src/main/java/com/clickhouse/jdbc/JdbcTypeMapping.java#L39-L176>`_
* `JDBC -> Spark <https://github.com/apache/spark/blob/v3.5.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L307>`_
* `Spark -> JDBC <https://github.com/apache/spark/blob/v3.5.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L141-L164>`_
* `JDBC -> Clickhouse <https://github.com/ClickHouse/clickhouse-java/blob/0.3.2/clickhouse-jdbc/src/main/java/com/clickhouse/jdbc/JdbcTypeMapping.java#L185-L311>`_
