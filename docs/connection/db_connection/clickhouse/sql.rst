.. _clickhouse-sql:

Reading from Clickhouse using ``Clickhouse.sql``
================================================

.. warning::

    Please take into account :ref:`clickhouse-types`

:obj:`Clickhouse.sql <onetl.connection.db_connection.clickhouse.connection.Clickhouse.sql>` allows passing custom SQL query,
but does not support incremental strategies.

Method also accepts :obj:`JDBCReadOptions <onetl.connection.db_connection.jdbc.options.JDBCReadOptions>`.

Syntax support
--------------

Only queries with the following syntax are supported:

* ✅︎ ``SELECT ... FROM ...``
* ✅︎ ``WITH alias AS (...) SELECT ...``

Queries like ``SHOW ...`` are not supported.

This method also does not support multiple queries in the same operation, like ``SET ...; SELECT ...;``.

Examples
--------

.. code-block:: python

    from onetl.connection import Clickhouse

    clickhouse = Clickhouse(...)
    df = clickhouse.sql(
        """
        SELECT
            id,
            key,
            CAST(value AS String) value,
            updated_at
        FROM
            some.mytable
        WHERE
            key = 'something'
        """,
        options=Clickhouse.ReadOptions(partition_column="id", num_partitions=10),
    )

References
----------

.. currentmodule:: onetl.connection.db_connection.clickhouse.connection

.. automethod:: Clickhouse.sql
