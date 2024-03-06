.. _postgres-types:

Postgres <-> Spark type mapping
=================================

Type detection & casting
------------------------

Spark's DataFrames always have a ``schema`` which is a list of columns with corresponding Spark types. All operations on a column are performed using column type.

Reading from Postgres
~~~~~~~~~~~~~~~~~~~~~~~

This is how Postgres connector performs this:

* For each column in query result (``SELECT column1, column2, ... FROM table ...``) get column name and Postgres type.
* Find corresponding ``Postgres type (read)`` -> ``Spark type`` combination (see below) for each DataFrame column [1]_. If no combination is found, raise exception.
* Create DataFrame from query with specific column names and Spark types.

.. [1]
    All Postgres types that doesn't have corresponding Java type are converted to ``String``.

Writing to some existing Clickhuse table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is how Postgres connector performs this:

* Get names of columns in DataFrame. [1]_
* Perform ``SELECT * FROM table LIMIT 0`` query.
* Take only columns present in DataFrame (by name, case insensitive). For each found column get Clickhouse type.
* Find corresponding ``Spark type`` -> ``Postgres type (write)`` combination (see below) for each DataFrame column. If no combination is found, raise exception.
* If ``Postgres type (write)`` match ``Postgres type (read)``, no additional casts will be performed, DataFrame column will be written to Postgres as is.
* If ``Postgres type (write)`` does not match ``Postgres type (read)``, DataFrame column will be casted to target column type **on Postgres side**.
  For example, you can write column with text data to ``int`` column, if column contains valid integer values within supported value range and precision [3]_.

.. [2]
    This allows to write data to tables with ``DEFAULT`` and ``GENERATED`` columns - if DataFrame has no such column,
    it will be populated by Postgres.

.. [3]
    This is true only if conversion of ``T_df -> T_tbl`` target or source types is ``text`` or ``StringType()``. So it is possible to
    insert data back to column with original type, if data format is matching the column type.

    But other types cannot be silently converted, like ``bytea -> bit(N)``. This requires explicit casting, see `Manual conversion to string`_.

Create new table using Spark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::

    ABSOLUTELY NOT RECOMMENDED!

This is how Postgres connector performs this:

* Find corresponding ``Spark type`` -> ``Postgres type (create)`` combination (see below) for each DataFrame column. If no combination is found, raise exception.
* Generate DDL for creating table in Postgres, like ``CREATE TABLE (col1 ...)``, and run it.
* Write DataFrame to created table as is.

But Postgres connector support only limited number of types and almost no custom clauses (like ``PARTITION BY``, ``INDEX``, etc).
So instead of relying on Spark to create tables:

.. dropdown:: See example

    .. code:: python

        writer = DBWriter(
            connection=postgres,
            table="public.table",
            options=Postgres.WriteOptions(
                if_exists="append",
                createTableOptions="PARTITION BY RANGE (id)",
            ),
        )
        writer.run(df)

Always prefer creating table with desired DDL **BEFORE WRITING DATA**:

.. dropdown:: See example

    .. code:: python

        postgres.execute(
            """
            CREATE TABLE public.table AS (
                id bigint,
                business_dt timestamp(6),
                value json
            )
            PARTITION BY RANGE (Id)
            """,
        )

        writer = DBWriter(
            connection=postgres,
            table="public.table",
            options=Postgres.WriteOptions(if_exists="append"),
        )
        writer.run(df)

See Postgres `CREATE TABLE <https://www.postgresql.org/docs/current/sql-createtable.html>`_ documentation.

Supported types
---------------

References
~~~~~~~~~~

See `List of Postgres types <https://www.postgresql.org/docs/current/datatype.html>`_.

Here you can find source code with type conversions:

* `Postgres <-> JDBC <https://github.com/pgjdbc/pgjdbc/blob/REL42.6.0/pgjdbc/src/main/java/org/postgresql/jdbc/TypeInfoCache.java#L78-L112>`_
* `JDBC -> Spark <https://github.com/apache/spark/blob/v3.5.0/sql/core/src/main/scala/org/apache/spark/sql/jdbc/PostgresDialect.scala#L50-L106>`_
* `Spark -> JDBC <https://github.com/apache/spark/blob/ce5ddad990373636e94071e7cef2f31021add07b/sql/core/src/main/scala/org/apache/spark/sql/jdbc/PostgresDialect.scala#L116-L130>`_

Numeric types
~~~~~~~~~~~~~

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

.. [4]

    Postgres support decimal types with unlimited precision.

    But Spark's ``DecimalType(P, S)`` supports maximum ``P=38`` (128 bit). It is impossible to read, write or operate with values of larger precision,
    this leads to an exception.

.. [5]

    Postgres support decimal types with negative scale, like ``decimal(38, -10)``. Spark doesn't.

Temporal types
~~~~~~~~~~~~~~

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

.. [6]

    ``time`` type is the same as ``timestamp`` with date ``1970-01-01``. So instead of reading data from Postgres like ``23:59:59``
    it is actually read ``1970-01-01 23:59:59``, and vice versa.

String types
~~~~~~~~~~~~

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
+-----------------------------+-----------------------|                       |                         |
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

Binary types
~~~~~~~~~~~~

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


Struct types
~~~~~~~~~~~~

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

Network types
~~~~~~~~~~~~~

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

Geo types
~~~~~~~~~

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

Explicit type cast
-------------------

``DBReader``
~~~~~~~~~~~

It is possible to explicitly cast column of unsupported type using ``DBReader(columns=...)`` syntax.

For example, you can use ``CAST(column AS text)`` to convert data to string representation on Postgres side, and so it will be read as Spark's ``StringType()``.

It is also possible to use ``to_json`` Postgres function for convert column of any type to string representation,
and then parse this column on Spark side using `from_json <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html>`_:

.. code-block:: python

    from pyspark.sql.functions import from_json
    from pyspark.sql.types import IntegerType

    from onetl.connection import Postgres
    from onetl.db import DBReader

    postgres = Postgres(...)

    DBReader(
        connection=postgres,
        columns=[
            "id",
            "supported_column",
            "CAST(unsupported_column AS text) unsupported_column_str",
            # or
            "to_json(unsupported_column) unsupported_column_json",
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
        from_json(df.unsupported_column_json, schema).alias("parsed_json"),
    )

``DBWriter``
~~~~~~~~~~~~

It is always possible to convert data on Spark side to string, and then write it to ``text`` column in Postgres table.

Using ``to_json``
^^^^^^^^^^^^^^^^^

For example, you can convert data using `to_json <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html>`_ function.

.. code:: python

    from pyspark.sql.functions import to_json

    from onetl.connection import Postgres
    from onetl.db import DBReader

    postgres = Postgres(...)

    postgres.execute(
        """
        CREATE TABLE schema.target_table (
            id int,
            supported_column timestamp,
            unsupported_column_json jsonb -- any column type, actually
        )
        """,
    )

    write_df = df.select(
        df.id,
        df.supported_column,
        to_json(df.unsupported_column).alias("unsupported_column_json"),
    )

    writer = DBWriter(
        connection=postgres,
        target="schema.target_table",
    )
    writer.run(write_df)

Then you can parse this column on Postgres side (for example, by creating a view):

.. code-block:: sql

    SELECT
        id,
        supported_column,
        -- access some nested field
        unsupported_column_json->'field'
    FROM
        schema.target_table

To avoid casting the value on every table read you can use `GENERATED ALWAYS STORED <https://www.postgresql.org/docs/current/ddl-generated-columns.html>`_ column, but this requires 2x space (for original and parsed value).

Manual conversion to string
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Postgres connector also supports conversion text value directly to target column type, if this value has a proper format.

For example, you can write data like ``[123, 345)`` to ``int8range`` type because Postgres allows cast ``'[123, 345)'::int8range'``:

.. code:: python

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

This can be tricky to implement and may lead to longer write process.
But this does not require extra space on Postgres side, and allows to avoid explicit value cast on every table read.
