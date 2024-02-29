.. _greenplum-types:

Greenplum <-> Spark type mapping
=================================

Type detection & casting
------------------------

Spark's DataFrames always have a ``schema`` which is a list of columns with corresponding Spark types. All operations on a column are performed using column type.

Reading from Greenplum
~~~~~~~~~~~~~~~~~~~~~~~

This is how Greenplum connector performs this:

* Execute query ``SELECT * FROM table LIMIT 0`` [1]_.
* For each column in query result get column name and Greenplum type.
* Find corresponding ``Greenplum type (read)`` -> ``Spark type`` combination (see below) for each DataFrame column. If no combination is found, raise exception.
* Use Spark column projection and predicate pushdown features to build a final query.
* Create DataFrame from generated query with inferred schema.

.. [1]
    Yes, **all columns of a table**, not just selected ones.
    This means that if source table **contains** columns with unsupported type, the entire table cannot be read.

Writing to some existing Clickhuse table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is how Greenplum connector performs this:

* Get names of columns in DataFrame.
* Perform ``SELECT * FROM table LIMIT 0`` query.
* For each column in query result get column name and Greenplum type.
* Match table columns with DataFrame columns (by name, case insensitive).
  If some column is present only in target table, but not in DataFrame (like ``DEFAULT`` or ``SERIAL`` column), and vice versa, raise an exception.
  See `Write unsupported column type`_.
* Find corresponding ``Spark type`` -> ``Greenplumtype (write)`` combination (see below) for each DataFrame column. If no combination is found, raise exception.
* If ``Greenplumtype (write)`` match ``Greenplum type (read)``, no additional casts will be performed, DataFrame column will be written to Greenplum as is.
* If ``Greenplumtype (write)`` does not match ``Greenplum type (read)``, DataFrame column will be casted to target column type **on Greenplum side**. For example, you can write column with text data to ``json`` column which Greenplum connector currently does not support.

Create new table using Spark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::

    ABSOLUTELY NOT RECOMMENDED!

This is how Greenplum connector performs this:

* Find corresponding ``Spark type`` -> ``Greenplum type (create)`` combination (see below) for each DataFrame column. If no combination is found, raise exception.
* Generate DDL for creating table in Greenplum, like ``CREATE TABLE (col1 ...)``, and run it.
* Write DataFrame to created table as is.

More details `can be found here <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/write_to_gpdb.html>`_.

But Greenplum connector support only limited number of types and almost no custom clauses (like ``PARTITION BY``).
So instead of relying on Spark to create tables:

.. dropdown:: See example

    .. code:: python

        writer = DBWriter(
            connection=greenplum,
            table="public.table",
            options=Greenplum.WriteOptions(
                if_exists="append",
                # by default distribution is random
                distributedBy="id",
                # partitionBy is not supported
            ),
        )
        writer.run(df)

Always prefer creating table with desired DDL **BEFORE WRITING DATA**:

.. dropdown:: See example

    .. code:: python

        greenplum.execute(
            """
            CREATE TABLE public.table AS (
                id int32,
                business_dt timestamp(6),
                value json
            )
            PARTITION BY RANGE (business_dt)
            DISTRIBUTED BY id
            """,
        )

        writer = DBWriter(
            connection=greenplum,
            table="public.table",
            options=Greenplum.WriteOptions(if_exists="append"),
        )
        writer.run(df)

See Greenplum `CREATE TABLE <https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-sql_commands-CREATE_TABLE.html>`_ documentation.

Supported types
---------------

See `list of Greenplum types <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/reference-datatype_mapping.html>`_.

Numeric types
~~~~~~~~~~~~~

+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| Greenplum type (read)            | Spark type                        | Greenplumtype (write)         | Greenplum type (create) |
+==================================+===================================+===============================+=========================+
| ``decimal``                      | ``DecimalType(P=38, S=18)``       | ``decimal(P=38, S=18)``       | ``decimal`` (unbounded) |
+----------------------------------+-----------------------------------+-------------------------------+                         |
| ``decimal(P=0..38)``             | ``DecimalType(P=0..38, S=0)``     | ``decimal(P=0..38, S=0)``     |                         |
+----------------------------------+-----------------------------------+-------------------------------+                         |
| ``decimal(P=0..38, S=0..38)``    | ``DecimalType(P=0..38, S=0..38)`` | ``decimal(P=0..38, S=0..38)`` |                         |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``decimal(P=39.., S=0..)``       | unsupported [2]_                  |                               |                         |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``real``                         | ``FloatType()``                   | ``real``                      | ``real``                |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``double precision``             | ``DoubleType()``                  | ``double precision``          | ``double precision``    |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``-``                            | ``ByteType()``                    | unsupported                   | unsupported             |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``smallint``                     | ``ShortType()``                   | ``smallint``                  | ``smallint``            |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``integer``                      | ``IntegerType()``                 | ``integer``                   | ``integer``             |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``bigint``                       | ``LongType()``                    | ``bigint``                    | ``bigint``              |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+
| ``money``                        | unsupported                       |                               |                         |
+----------------------------------+                                   |                               |                         |
| ``int4range``                    |                                   |                               |                         |
+----------------------------------+                                   |                               |                         |
| ``int8range``                    |                                   |                               |                         |
+----------------------------------+                                   |                               |                         |
| ``numrange``                     |                                   |                               |                         |
+----------------------------------+                                   |                               |                         |
| ``int2vector``                   |                                   |                               |                         |
+----------------------------------+-----------------------------------+-------------------------------+-------------------------+

.. [2]

    Greenplum support decimal types with unlimited precision.

    But Spark's ``DecimalType(P, S)`` supports maximum ``P=38`` (128 bit). It is impossible to read, write or operate with values of larger precision,
    this leads to an exception.

Temporal types
~~~~~~~~~~~~~~

+------------------------------------+-------------------------+-----------------------+-------------------------+
| Greenplum type (read)              | Spark type              | Greenplumtype (write) | Greenplum type (create) |
+====================================+=========================+=======================+=========================+
| ``date``                           | ``DateType()``          | ``date``              | ``date``                |
+------------------------------------+-------------------------+-----------------------+-------------------------+
| ``time``                           | ``TimestampType()``,    | ``timestamp``         | ``timestamp``           |
+------------------------------------+ time format quirks [3]_ |                       |                         |
| ``time(0..6)``                     |                         |                       |                         |
+------------------------------------+                         |                       |                         |
| ``time with time zone``            |                         |                       |                         |
+------------------------------------+                         |                       |                         |
| ``time(0..6) with time zone``      |                         |                       |                         |
+------------------------------------+-------------------------+-----------------------+-------------------------+
| ``timestamp``                      | ``TimestampType()``     | ``timestamp``         | ``timestamp``           |
+------------------------------------+                         |                       |                         |
| ``timestamp(0..6)``                |                         |                       |                         |
+------------------------------------+                         |                       |                         |
| ``timestamp with time zone``       |                         |                       |                         |
+------------------------------------+                         |                       |                         |
| ``timestamp(0..6) with time zone`` |                         |                       |                         |
+------------------------------------+-------------------------+-----------------------+-------------------------+
| ``interval`` or any precision      | unsupported             |                       |                         |
+------------------------------------+                         |                       |                         |
| ``daterange``                      |                         |                       |                         |
+------------------------------------+                         |                       |                         |
| ``tsrange``                        |                         |                       |                         |
+------------------------------------+                         |                       |                         |
| ``tstzrange``                      |                         |                       |                         |
+------------------------------------+-------------------------+-----------------------+-------------------------+

.. [3]

    ``time`` type is the same as ``timestamp`` with date ``1970-01-01``. So instead of reading data from Postgres like ``23:59:59``
    it is actually read ``1970-01-01 23:59:59``, and vice versa.

String types
~~~~~~~~~~~~

+-----------------------------+------------------+-----------------------+-------------------------+
| Greenplum type (read)       | Spark type       | Greenplumtype (write) | Greenplum type (create) |
+=============================+==================+=======================+=========================+
| ``character``               | ``StringType()`` | ``text``              | ``text``                |
+-----------------------------+                  |                       |                         |
| ``character(N)``            |                  |                       |                         |
+-----------------------------+                  |                       |                         |
| ``character varying``       |                  |                       |                         |
+-----------------------------+                  |                       |                         |
| ``character varying(N)``    |                  |                       |                         |
+-----------------------------+                  |                       |                         |
| ``text``                    |                  |                       |                         |
+-----------------------------+                  |                       |                         |
| ``xml``                     |                  |                       |                         |
+-----------------------------+                  |                       |                         |
| ``CREATE TYPE ... AS ENUM`` |                  |                       |                         |
+-----------------------------+------------------+-----------------------+-------------------------+
| ``json``                    | unsupported      |                       |                         |
+-----------------------------+                  |                       |                         |
| ``jsonb``                   |                  |                       |                         |
+-----------------------------+------------------+-----------------------+-------------------------+

Binary types
~~~~~~~~~~~~

+--------------------------+-------------------+-----------------------+-------------------------+
| Greenplum type (read)    | Spark type        | Greenplumtype (write) | Greenplum type (create) |
+==========================+===================+=======================+=========================+
| ``boolean``              | ``BooleanType()`` | ``boolean``           | ``boolean``             |
+--------------------------+-------------------+-----------------------+-------------------------+
| ``bit``                  | unsupported       |                       |                         |
+--------------------------+                   |                       |                         |
| ``bit(N)``               |                   |                       |                         |
+--------------------------+                   |                       |                         |
| ``bit varying``          |                   |                       |                         |
+--------------------------+                   |                       |                         |
| ``bit varying(N)``       |                   |                       |                         |
+--------------------------+-------------------+-----------------------+-------------------------+
| ``bytea``                | unsupported [4]_  |                       |                         |
+--------------------------+-------------------+-----------------------+-------------------------+
| ``-``                    | ``BinaryType()``  | ``bytea``             | ``bytea``               |
+--------------------------+-------------------+-----------------------+-------------------------+

.. [4] Yes, that's weird.

Struct types
~~~~~~~~~~~~

+--------------------------------+------------------+-----------------------+-------------------------+
| Greenplum type (read)          | Spark type       | Greenplumtype (write) | Greenplum type (create) |
+================================+==================+=======================+=========================+
| ``T[]``                        | unsupported      |                       |                         |
+--------------------------------+------------------+-----------------------+-------------------------+
| ``-``                          | ``ArrayType()``  | unsupported           |                         |
+--------------------------------+------------------+-----------------------+-------------------------+
| ``CREATE TYPE sometype (...)`` | ``StringType()`` | ``text``              | ``text``                |
+--------------------------------+------------------+-----------------------+-------------------------+
| ``-``                          | ``StructType()`` | unsupported           |                         |
+--------------------------------+------------------+                       |                         |
| ``-``                          | ``MapType()``    |                       |                         |
+--------------------------------+------------------+-----------------------+-------------------------+

Unsupported types
-----------------

Columns of these types cannot be read/written by Spark:
    * ``cidr``
    * ``inet``
    * ``macaddr``
    * ``macaddr8``
    * ``circle``
    * ``box``
    * ``line``
    * ``lseg``
    * ``path``
    * ``point``
    * ``polygon``
    * ``tsvector``
    * ``tsquery``
    * ``uuid``

The is a way to avoid this - just cast unsupported types to ``text``. But the way this can be done is not a straightforward.

Read unsupported column type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Unfortunately, it is not possible to cast unsupported column to some supported type on ``DBReader`` side:

.. code-block:: python

    DBReader(
        connection=greenplum,
        # will fail
        columns=["CAST(column AS text)"],
    )

This is related to Greenplum connector implementation. Instead of passing this ``CAST`` expression to ``SELECT`` query
as is, it performs type cast on Spark side, so this syntax is not supported.

But there is a workaround - create a view with casting unsupported column to ``text`` (or any other supported type).

For example, you can use ``to_json`` Postgres function for convert column of any type to string representation.
You can then parse this column on Spark side using `from_json <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html>`_:

.. code:: python

    from pyspark.sql.functions import from_json
    from pyspark.sql.types import ArrayType, IntegerType

    from onetl.connection import Greenplum
    from onetl.db import DBReader

    greenplum = Greenplum(...)

    # create view with proper type cast
    greenplum.execute(
        """
        CREATE VIEW schema.view_with_json_column AS
        SELECT
            id,
            supported_column,
            to_json(array_column) array_column_as_json,
            gp_segment_id  -- ! important !
        FROM
            schema.table_with_unsupported_columns
        """,
    )

    # create dataframe using this view
    reader = DBReader(
        connection=greenplum,
        source="schema.view_with_json_column",
    )
    df = reader.run()

    # Spark requires all columns to have some type, describe it
    column_type = ArrayType(IntegerType())

    # cast column content to proper Spark type
    df = df.select(
        df.id,
        df.supported_column,
        from_json(df.array_column_as_json, schema).alias("array_column"),
    )

Write unsupported column type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It is always possible to convert data on Spark side to string, and then write it to ``text`` column in Greenplum table.

For example, you can convert data using `to_json <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html>`_ function.

.. code:: python

    from pyspark.sql.functions import to_json

    from onetl.connection import Greenplum
    from onetl.db import DBReader

    greenplum = Greenplum(...)

    greenplum.execute(
        """
        CREATE TABLE schema.target_table (
            id int,
            supported_column timestamp,
            array_column_as_json jsonb, -- or text
        )
        DISTRIBUTED BY id
        """,
    )

    write_df = df.select(
        df.id,
        df.supported_column,
        to_json(df.array_column).alias("array_column_json"),
    )

    writer = DBWriter(
        connection=greenplum,
        target="schema.target_table",
    )
    writer.run(write_df)

Then you can parse this column on Greenplum side:

.. code-block:: sql

    SELECT
        id,
        supported_column,
        -- access first item of an array
        array_column_as_json->0
    FROM
        schema.target_table
