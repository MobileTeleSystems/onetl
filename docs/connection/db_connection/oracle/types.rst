.. _oracle-types:

Oracle <-> Spark type mapping
=============================

Type detection & casting
------------------------

Spark's DataFrames always have a ``schema`` which is a list of columns with corresponding Spark types. All operations on a column are performed using column type.

Reading from Oracle
~~~~~~~~~~~~~~~~~~~

This is how Oracle connector performs this:

* For each column in query result (``SELECT column1, column2, ... FROM table ...``) get column name and Oracle type.
* Find corresponding ``Oracle type (read)`` -> ``Spark type`` combination (see below) for each DataFrame column. If no combination is found, raise exception.
* Create DataFrame from query with specific column names and Spark types.

Writing to some existing Oracle table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is how Oracle connector performs this:

* Get names of columns in DataFrame. [1]_
* Perform ``SELECT * FROM table LIMIT 0`` query.
* Take only columns present in DataFrame (by name, case insensitive). For each found column get Clickhouse type.
* **Find corresponding** ``Oracle type (read)`` -> ``Spark type`` **combination** (see below) for each DataFrame column. If no combination is found, raise exception. [2]_
* Find corresponding ``Spark type`` -> ``Oracle type (write)`` combination (see below) for each DataFrame column. If no combination is found, raise exception.
* If ``Oracle type (write)`` match ``Oracle type (read)``, no additional casts will be performed, DataFrame column will be written to Oracle as is.
* If ``Oracle type (write)`` does not match ``Oracle type (read)``, DataFrame column will be casted to target column type **on Oracle side**.
  For example, you can write column with text data to ``int`` column, if column contains valid integer values within supported value range and precision.

.. [1]
    This allows to write data to tables with ``DEFAULT`` and ``GENERATED`` columns - if DataFrame has no such column,
    it will be populated by Oracle.

.. [2]

    Yes, this is weird.

Create new table using Spark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::

    ABSOLUTELY NOT RECOMMENDED!

This is how Oracle connector performs this:

* Find corresponding ``Spark type`` -> ``Oracle type (create)`` combination (see below) for each DataFrame column. If no combination is found, raise exception.
* Generate DDL for creating table in Oracle, like ``CREATE TABLE (col1 ...)``, and run it.
* Write DataFrame to created table as is.

But Oracle connector support only limited number of types and almost no custom clauses (like ``PARTITION BY``, ``INDEX``, etc).
So instead of relying on Spark to create tables:

.. dropdown:: See example

    .. code:: python

        writer = DBWriter(
            connection=oracle,
            table="public.table",
            options=Oracle.WriteOptions(if_exists="append"),
        )
        writer.run(df)

Always prefer creating table with desired DDL **BEFORE WRITING DATA**:

.. dropdown:: See example

    .. code:: python

        oracle.execute(
            """
            CREATE TABLE username.table AS (
                id NUMBER,
                business_dt TIMESTAMP(6),
                value VARCHAR2(2000)
            )
            """,
        )

        writer = DBWriter(
            connection=oracle,
            table="public.table",
            options=Oracle.WriteOptions(if_exists="append"),
        )
        writer.run(df)

See Oracle `CREATE TABLE <https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/SELECT.html>`_ documentation.

Supported types
---------------

References
~~~~~~~~~~

See `List of Oracle types <https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/Data-Types.html>`_.

Here you can find source code with type conversions:

* `JDBC -> Spark <https://github.com/apache/spark/blob/v3.5.0/sql/core/src/main/scala/org/apache/spark/sql/jdbc/OracleDialect.scala#L83-L109>`_
* `Spark -> JDBC <https://github.com/apache/spark/blob/v3.5.0/sql/core/src/main/scala/org/apache/spark/sql/jdbc/OracleDialect.scala#L111-L123>`_

Numeric types
~~~~~~~~~~~~~

+----------------------------------+-----------------------------------+-------------------------------+---------------------------+
| Oracle type (read)               | Spark type                        | Oracle type (write)           | Oracle type (create)      |
+==================================+===================================+===============================+===========================+
| ``NUMBER``                       | ``DecimalType(P=38, S=10)``       | ``NUMBER(P=38, S=10)``        | ``NUMBER(P=38, S=10)``    |
+----------------------------------+-----------------------------------+-------------------------------+---------------------------+
| ``NUMBER(P=0..38)``              | ``DecimalType(P=0..38, S=0)``     | ``NUMBER(P=0..38, S=0)``      | ``NUMBER(P=38, S=0)``     |
+----------------------------------+-----------------------------------+-------------------------------+---------------------------+
| ``NUMBER(P=0..38, S=0..38)``     | ``DecimalType(P=0..38, S=0..38)`` | ``NUMBER(P=0..38, S=0..38)``  | ``NUMBER(P=38, S=0..38)`` |
+----------------------------------+-----------------------------------+-------------------------------+---------------------------+
| ``NUMBER(P=..., S=-127..-1)``    | unsupported [3]_                  |                               |                           |
+----------------------------------+-----------------------------------+-------------------------------+---------------------------+
| ``FLOAT``                        | ``DecimalType(P=38, S=10)``       | ``NUMBER(P=38, S=10)``        | ``NUMBER(P=38, S=10)``    |
+----------------------------------+                                   |                               |                           |
| ``FLOAT(N)``                     |                                   |                               |                           |
+----------------------------------+                                   |                               |                           |
| ``REAL``                         |                                   |                               |                           |
+----------------------------------+                                   |                               |                           |
| ``DOUBLE PRECISION``             |                                   |                               |                           |
+----------------------------------+-----------------------------------+-------------------------------+---------------------------+
| ``BINARY_FLOAT``                 | ``FloatType()``                   | ``NUMBER(P=19, S=4)``         | ``NUMBER(P=19, S=4)``     |
+----------------------------------+-----------------------------------+                               |                           |
| ``BINARY_DOUBLE``                | ``DoubleType()``                  |                               |                           |
+----------------------------------+-----------------------------------+-------------------------------+---------------------------+
| ``SMALLINT``                     | ``DecimalType(P=38, S=0)``        | ``NUMBER(P=38, S=0)``         | ``NUMBER(P=38, S=0)``     |
+----------------------------------+                                   |                               |                           |
| ``INTEGER``                      |                                   |                               |                           |
+----------------------------------+-----------------------------------+-------------------------------+---------------------------+
| ``LONG``                         | ``StringType()``                  | ``CLOB``                      | ``CLOB``                  |
+----------------------------------+-----------------------------------+-------------------------------+---------------------------+

.. [3]

    Oracle support decimal types with negative scale, like ``NUMBER(38, -10)``. Spark doesn't.

Temporal types
~~~~~~~~~~~~~~

+--------------------------------------------+------------------------------------+---------------------------------+---------------------------------+
| Oracle type (read)                         | Spark type                         | Oracle type (write)             | Oracle type (create)            |
+============================================+====================================+=================================+=================================+
| ``DATE``, days                             | ``TimestampType()``, microseconds  | ``TIMESTAMP(6)``, microseconds  | ``TIMESTAMP(6)``, microseconds  |
+--------------------------------------------+------------------------------------+---------------------------------+---------------------------------+
| ``TIMESTAMP``, microseconds                | ``TimestampType()``, microseconds  | ``TIMESTAMP(6)``, microseconds  | ``TIMESTAMP(6)``, microseconds  |
+--------------------------------------------+                                    |                                 |                                 |
| ``TIMESTAMP(0)``, seconds                  |                                    |                                 |                                 |
+--------------------------------------------+                                    |                                 |                                 |
| ``TIMESTAMP(3)``, milliseconds             |                                    |                                 |                                 |
+--------------------------------------------+                                    |                                 |                                 |
| ``TIMESTAMP(6)``, microseconds             |                                    |                                 |                                 |
+--------------------------------------------+------------------------------------+---------------------------------+---------------------------------+
| ``TIMESTAMP(9)``, nanoseconds              | ``TimestampType()``, microseconds, | ``TIMESTAMP(6)``, microseconds, | ``TIMESTAMP(6)``, microseconds, |
|                                            | **precision loss** [4]_            | **precision loss**              | **precision loss**              |
+--------------------------------------------+------------------------------------+---------------------------------+---------------------------------+
| ``TIMESTAMP WITH TIME ZONE``               | unsupported                        |                                 |                                 |
+--------------------------------------------+                                    |                                 |                                 |
| ``TIMESTAMP(N) WITH TIME ZONE``            |                                    |                                 |                                 |
+--------------------------------------------+                                    |                                 |                                 |
| ``TIMESTAMP WITH LOCAL TIME ZONE``         |                                    |                                 |                                 |
+--------------------------------------------+                                    |                                 |                                 |
| ``TIMESTAMP(N) WITH LOCAL TIME ZONE``      |                                    |                                 |                                 |
+--------------------------------------------+                                    |                                 |                                 |
| ``INTERVAL YEAR TO MONTH``                 |                                    |                                 |                                 |
+--------------------------------------------+                                    |                                 |                                 |
| ``INTERVAL DAY TO SECOND``                 |                                    |                                 |                                 |
+--------------------------------------------+------------------------------------+---------------------------------+---------------------------------+

.. warning::

    Note that types in Oracle and Spark have different value ranges:

    +---------------+------------------------------------+-----------------------------------+---------------------+--------------------------------+--------------------------------+
    | Oracle type   | Min value                          | Max value                         | Spark type          | Min value                      | Max value                      |
    +===============+====================================+===================================+=====================+================================+================================+
    | ``date``      | ``-4712-01-01``                    | ``9999-01-01``                    | ``DateType()``      | ``0001-01-01``                 | ``9999-12-31``                 |
    +---------------+------------------------------------+-----------------------------------+---------------------+--------------------------------+--------------------------------+
    | ``timestamp`` | ``-4712-01-01 00:00:00.000000000`` | ``9999-12-31 23:59:59.999999999`` | ``TimestampType()`` | ``0001-01-01 00:00:00.000000`` | ``9999-12-31 23:59:59.999999`` |
    +---------------+------------------------------------+-----------------------------------+---------------------+--------------------------------+--------------------------------+

    So not all of values can be read from Oracle to Spark.

    References:
        * `Oracle date, timestamp and intervals documentation <https://oracle-base.com/articles/misc/oracle-dates-timestamps-and-intervals#DATE>`_
        * `Spark DateType documentation <https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html>`_
        * `Spark TimestampType documentation <https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html>`_

.. [4]
    Oracle support timestamp up to nanoseconds precision (``23:59:59.999999999``),
    but Spark ``TimestampType()`` supports datetime up to microseconds precision (``23:59:59.999999``).
    Nanoseconds will be lost during read or write operations.

String types
~~~~~~~~~~~~

+-----------------------------+------------------+---------------------+----------------------+
| Oracle type (read)          | Spark type       | Oracle type (write) | Oracle type (create) |
+=============================+==================+=====================+======================+
| ``CHAR``                    | ``StringType()`` | ``CLOB``            | ``CLOB``             |
+-----------------------------+                  |                     |                      |
| ``CHAR(N CHAR)``            |                  |                     |                      |
+-----------------------------+                  |                     |                      |
| ``CHAR(N BYTE)``            |                  |                     |                      |
+-----------------------------+                  |                     |                      |
| ``NCHAR``                   |                  |                     |                      |
+-----------------------------+                  |                     |                      |
| ``NCHAR(N)``                |                  |                     |                      |
+-----------------------------+                  |                     |                      |
| ``VARCHAR(N)``              |                  |                     |                      |
+-----------------------------+                  |                     |                      |
| ``LONG VARCHAR``            |                  |                     |                      |
+-----------------------------+                  |                     |                      |
| ``VARCHAR2(N CHAR)``        |                  |                     |                      |
+-----------------------------+                  |                     |                      |
| ``VARCHAR2(N BYTE)``        |                  |                     |                      |
+-----------------------------+                  |                     |                      |
| ``NVARCHAR2(N)``            |                  |                     |                      |
+-----------------------------+                  |                     |                      |
| ``CLOB``                    |                  |                     |                      |
+-----------------------------+                  |                     |                      |
| ``NCLOB``                   |                  |                     |                      |
+-----------------------------+------------------+---------------------+----------------------+

Binary types
~~~~~~~~~~~~

+--------------------------+------------------+---------------------+----------------------+
| Oracle type (read)       | Spark type       | Oracle type (write) | Oracle type (create) |
+==========================+==================+=====================+======================+
| ``RAW(N)``               | ``BinaryType()`` | ``BLOB``            | ``BLOB``             |
+--------------------------+                  |                     |                      |
| ``LONG RAW``             |                  |                     |                      |
+--------------------------+                  |                     |                      |
| ``BLOB``                 |                  |                     |                      |
+--------------------------+------------------+---------------------+----------------------+
| ``BFILE``                | unsupported      |                     |                      |
+--------------------------+------------------+---------------------+----------------------+

Struct types
~~~~~~~~~~~~

+-------------------------------------+------------------+---------------------+----------------------+
| Oracle type (read)                  | Spark type       | Oracle type (write) | Oracle type (create) |
+=====================================+==================+=====================+======================+
| ``XMLType``                         | ``StringType()`` | ``CLOB``            | ``CLOB``             |
+-------------------------------------+                  |                     |                      |
| ``URIType``                         |                  |                     |                      |
+-------------------------------------+                  |                     |                      |
| ``DBURIType``                       |                  |                     |                      |
+-------------------------------------+                  |                     |                      |
| ``XDBURIType``                      |                  |                     |                      |
+-------------------------------------+                  |                     |                      |
| ``HTTPURIType``                     |                  |                     |                      |
+-------------------------------------+                  |                     |                      |
| ``CREATE TYPE ... AS OBJECT (...)`` |                  |                     |                      |
+-------------------------------------+------------------+---------------------+----------------------+
| ``JSON``                            | unsupported      |                     |                      |
+-------------------------------------+                  |                     |                      |
| ``CREATE TYPE ... AS VARRAY ...``   |                  |                     |                      |
+-------------------------------------+                  |                     |                      |
| ``CREATE TYPE ... AS TABLE OF ...`` |                  |                     |                      |
+-------------------------------------+------------------+---------------------+----------------------+

Special types
~~~~~~~~~~~~~

+--------------------+-------------------+---------------------+----------------------+
| Oracle type (read) | Spark type        | Oracle type (write) | Oracle type (create) |
+====================+===================+=====================+======================+
| ``BOOLEAN``        | ``BooleanType()`` | ``BOOLEAN``         | ``NUMBER(P=1, S=0)`` |
+--------------------+-------------------+---------------------+----------------------+
| ``ROWID``          | ``StringType()``  | ``CLOB``            | ``CLOB``             |
+--------------------+                   |                     |                      |
| ``UROWID``         |                   |                     |                      |
+--------------------+                   |                     |                      |
| ``UROWID(N)``      |                   |                     |                      |
+--------------------+-------------------+---------------------+----------------------+
| ``ANYTYPE``        | unsupported       |                     |                      |
+--------------------+                   |                     |                      |
| ``ANYDATA``        |                   |                     |                      |
+--------------------+                   |                     |                      |
| ``ANYDATASET``     |                   |                     |                      |
+--------------------+-------------------+---------------------+----------------------+

Explicit type cast
------------------

``DBReader``
~~~~~~~~~~~~

It is possible to explicitly cast column of unsupported type using ``DBReader(columns=...)`` syntax.

For example, you can use ``CAST(column AS CLOB)`` to convert data to string representation on Oracle side, and so it will be read as Spark's ``StringType()``.

It is also possible to use `JSON_ARRAY <https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/JSON_ARRAY.html>`_
or `JSON_OBJECT <https://docs.oracle.com/en/database/oracle/oracle-database/23/sqlrf/JSON_OBJECT.html>`_ Oracle functions
to convert column of any type to string representation, and then parse this column on Spark side using
`from_json <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html>`_:

.. code-block:: python

    from pyspark.sql.types import IntegerType

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

    # Spark requires all columns to have some type, describe it
    column_type = IntegerType()

    # cast column content to proper Spark type
    df = df.select(
        df.id,
        df.supported_column,
        # explicit cast
        df.unsupported_column_str.cast("integer").alias("parsed_integer"),
        # or explicit json parsing
        from_json(df.array_column_json, schema).alias("array_column"),
    )

``DBWriter``
~~~~~~~~~~~~

It is always possible to convert data on Spark side to string, and then write it to ``text`` column in Oracle table.

For example, you can convert data using `to_json <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html>`_ function.

.. code:: python

    from pyspark.sql.functions import to_json

    from onetl.connection import Oracle
    from onetl.db import DBReader

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
        to_json(df.unsupported_column).alias("array_column_json"),
    )

    writer = DBWriter(
        connection=oracle,
        target="schema.target_table",
    )
    writer.run(write_df)

Then you can parse this column on Oracle side - for example, by creating a view:

.. code-block:: sql

    SELECT
        id,
        supported_column,
        JSON_VALUE(array_column_json, '$[0]' RETURNING NUMBER) AS array_item_0
    FROM
        schema.target_table

Or by using `VIRTUAL column <https://oracle-base.com/articles/11g/virtual-columns-11gr1>`_:

.. code-block:: sql

    CREATE TABLE schema.target_table (
        id INTEGER,
        supported_column TIMESTAMP,
        array_column_json VARCHAR2(4000), -- any string type, actually
        array_item_0 GENERATED ALWAYS AS (JSON_VALUE(array_column_json, '$[0]' RETURNING NUMBER)) VIRTUAL
    )

But data will be parsed on each table read in any case, as Oracle does no support ``GENERATED ALWAYS AS (...) STORED`` columns.
