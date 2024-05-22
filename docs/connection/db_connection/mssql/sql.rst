.. _mssql-sql:

Reading from MSSQL using ``MSSQL.sql``
========================================

``MSSQL.sql`` allows passing custom SQL query, but does not support incremental strategies.

.. warning::

    Please take into account :ref:`mssql-types`

.. warning::

    Statement is executed in **read-write** connection, so if you're calling some functions/procedures with DDL/DML statements inside,
    they can change data in your database.

Syntax support
--------------

Only queries with the following syntax are supported:

* ✅︎ ``SELECT ... FROM ...``
* ❌ ``WITH alias AS (...) SELECT ...``
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
--------

.. code-block:: python

    from onetl.connection import MSSQL

    mssql = MSSQL(...)
    df = mssql.sql(
        """
        SELECT
            id,
            key,
            CAST(value AS text) value,
            updated_at
        FROM
            some.mytable
        WHERE
            key = 'something'
        """,
        options=MSSQL.SQLOptions(
            partition_column="id",
            num_partitions=10,
            lower_bound=0,
            upper_bound=1000,
        ),
    )

Recommendations
---------------

Select only required columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instead of passing ``SELECT * FROM ...`` prefer passing exact column names ``SELECT col1, col2, ...``.
This reduces the amount of data passed from MSSQL to Spark.

Pay attention to ``where`` value
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instead of filtering data on Spark side using ``df.filter(df.column == 'value')`` pass proper ``WHERE column = 'value'`` clause.
This both reduces the amount of data send from MSSQL to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in ``where`` clause.

Options
-------

.. currentmodule:: onetl.connection.db_connection.mssql.options

.. autopydantic_model:: MSSQLSQLOptions
    :inherited-members: GenericOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
