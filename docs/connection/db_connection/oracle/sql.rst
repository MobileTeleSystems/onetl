.. _oracle-sql:

Reading from Oracle using ``Oracle.sql``
========================================

.. warning::

    Please take into account :ref:`oracle-types`

:obj:`Oracle.sql <onetl.connection.db_connection.oracle.connection.Oracle.sql>` allows passing custom SQL query,
but does not support incremental strategies.

Method also accepts :obj:`JDBCReadOptions <onetl.connection.db_connection.jdbc.options.JDBCReadOptions>`.

Syntax support
--------------

Only queries with the following syntax are supported:

* ``SELECT ...``
* ``WITH alias AS (...) SELECT ...``

Queries like ``SHOW ...`` are not supported.

This method also does not support multiple queries in the same operation, like ``SET ...; SELECT ...;``.

Examples
--------

.. code-block:: python

    from onetl.connection import Oracle

    oracle = Oracle(...)
    df = oracle.sql(
        """
        SELECT
            id,
            key,
            CAST(value AS VARCHAR2(4000)) value,
            updated_at
        FROM
            some.mytable
        WHERE
            key = 'something'
        """,
        options=Oracle.ReadOptions(partition_column="id", num_partitions=10),
    )

References
----------

.. currentmodule:: onetl.connection.db_connection.oracle.connection

.. automethod:: Oracle.sql
