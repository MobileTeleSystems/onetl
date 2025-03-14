.. _greenplum-types:

Greenplum <-> Spark type mapping
=================================

.. note::

    The results below are valid for Spark 3.2.4, and may differ on other Spark versions.

Type detection & casting
------------------------

Spark's DataFrames always have a ``schema`` which is a list of columns with corresponding Spark types. All operations on a column are performed using column type.

Reading from Greenplum
~~~~~~~~~~~~~~~~~~~~~~~

This is how Greenplum connector performs this:

* Execute query ``SELECT * FROM table LIMIT 0`` [1]_.
* For each column in query result get column name and Greenplum type.
* Find corresponding ``Greenplum type (read)`` → ``Spark type`` combination (see below) for each DataFrame column. If no combination is found, raise exception.
* Use Spark column projection and predicate pushdown features to build a final query.
* Create DataFrame from generated query with inferred schema.

.. [1]
    Yes, **all columns of a table**, not just selected ones.
    This means that if source table **contains** columns with unsupported type, the entire table cannot be read.

Writing to some existing Greenplum table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This is how Greenplum connector performs this:

* Get names of columns in DataFrame.
* Perform ``SELECT * FROM table LIMIT 0`` query.
* For each column in query result get column name and Greenplum type.
* Match table columns with DataFrame columns (by name, case insensitive).
  If some column is present only in target table, but not in DataFrame (like ``DEFAULT`` or ``SERIAL`` column), and vice versa, raise an exception.
  See `Explicit type cast`_.
* Find corresponding ``Spark type`` → ``Greenplumtype (write)`` combination (see below) for each DataFrame column. If no combination is found, raise exception.
* If ``Greenplumtype (write)`` match ``Greenplum type (read)``, no additional casts will be performed, DataFrame column will be written to Greenplum as is.
* If ``Greenplumtype (write)`` does not match ``Greenplum type (read)``, DataFrame column will be casted to target column type **on Greenplum side**. For example, you can write column with text data to ``json`` column which Greenplum connector currently does not support.

Create new table using Spark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::

    ABSOLUTELY NOT RECOMMENDED!

This is how Greenplum connector performs this:

* Find corresponding ``Spark type`` → ``Greenplum type (create)`` combination (see below) for each DataFrame column. If no combination is found, raise exception.
* Generate DDL for creating table in Greenplum, like ``CREATE TABLE (col1 ...)``, and run it.
* Write DataFrame to created table as is.

More details `can be found here <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/write_to_gpdb.html>`_.

But Greenplum connector support only limited number of types and almost no custom clauses (like ``PARTITION BY``).
So instead of relying on Spark to create tables:

.. dropdown:: See example

    .. code:: python

        writer = DBWriter(
            connection=greenplum,
            target="public.table",
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
            CREATE TABLE public.table (
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
            target="public.table",
            options=Greenplum.WriteOptions(if_exists="append"),
        )
        writer.run(df)

See Greenplum `CREATE TABLE <https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-sql_commands-CREATE_TABLE.html>`_ documentation.

Supported types
---------------

See:
    * `official connector documentation <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/reference-datatype_mapping.html>`_
    * `list of Greenplum types <https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-data_types.html>`_

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

.. warning::

    Note that types in Greenplum and Spark have different value ranges:

    +----------------+---------------------------------+----------------------------------+---------------------+--------------------------------+--------------------------------+
    | Greenplum type | Min value                       | Max value                        | Spark type          | Min value                      | Max value                      |
    +================+=================================+==================================+=====================+================================+================================+
    | ``date``       | ``-4713-01-01``                 | ``5874897-01-01``                | ``DateType()``      | ``0001-01-01``                 | ``9999-12-31``                 |
    +----------------+---------------------------------+----------------------------------+---------------------+--------------------------------+--------------------------------+
    | ``timestamp``  | ``-4713-01-01 00:00:00.000000`` | ``294276-12-31 23:59:59.999999`` | ``TimestampType()`` | ``0001-01-01 00:00:00.000000`` | ``9999-12-31 23:59:59.999999`` |
    +----------------+---------------------------------+----------------------------------+                     |                                |                                |
    | ``time``       | ``00:00:00.000000``             | ``24:00:00.000000``              |                     |                                |                                |
    +----------------+---------------------------------+----------------------------------+---------------------+--------------------------------+--------------------------------+

    So not all of values can be read from Greenplum to Spark.

    References:
        * `Greenplum types documentation <https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-data_types.html>`_
        * `Spark DateType documentation <https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html>`_
        * `Spark TimestampType documentation <https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html>`_

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

Explicit type cast
------------------

``DBReader``
~~~~~~~~~~~~

Direct casting of Greenplum types is not supported by DBReader due to the connector’s implementation specifics.

.. code-block:: python

    reader = DBReader(
        connection=greenplum,
        # will fail
        columns=["CAST(unsupported_column AS text)"],
    )

But there is a workaround - create a view with casting unsupported column to text (or any other supported type).
For example, you can use `to_json <https://www.postgresql.org/docs/current/functions-json.html>`_ Postgres function to convert column of any type to string representation and then parse this column on Spark side using :obj:`JSON.parse_column <onetl.file.format.json.JSON.parse_column>` method.

.. code-block:: python

    from pyspark.sql.types import ArrayType, IntegerType

    from onetl.connection import Greenplum
    from onetl.db import DBReader
    from onetl.file.format import JSON

    greenplum = Greenplum(...)

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

    # Define the schema for the JSON data
    json_scheme = ArrayType(IntegerType())

    df = df.select(
        df.id,
        df.supported_column,
        JSON().parse_column(df.array_column_as_json, json_scheme).alias("array_column"),
    )

``DBWriter``
~~~~~~~~~~~~

To write data to a ``text`` or ``json`` column in a Greenplum table, use :obj:`JSON.serialize_column <onetl.file.format.json.JSON.serialize_column>` method.

.. code-block:: python


    from onetl.connection import Greenplum
    from onetl.db import DBWriter
    from onetl.file.format import JSON

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
        JSON().serialize_column(df.array_column).alias("array_column_json"),
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
