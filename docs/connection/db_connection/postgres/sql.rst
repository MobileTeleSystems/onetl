.. _postgres-sql:

Reading from Postgres using ``Postgres.sql``
============================================

.. warning::

    Please take into account :ref:`postgres-types`

:obj:`Postgres.sql <onetl.connection.db_connection.postgres.connection.Postgres.sql>` allows passing custom SQL query,
but does not support incremental strategies.

Method also accepts :obj:`JDBCReadOptions <onetl.connection.db_connection.jdbc.options.JDBCReadOptions>`.

Syntax support
--------------

Only queries with the following syntax are supported:

* ✅︎ ``SELECT ... FROM ...``
* ✅︎ ``WITH alias AS (...) SELECT ...``
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
--------

.. code-block:: python

    from onetl.connection import Postgres

    postgres = Postgres(...)
    df = postgres.sql(
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
        options=Postgres.ReadOptions(partition_column="id", num_partitions=10),
    )

References
----------

.. currentmodule:: onetl.connection.db_connection.postgres.connection

.. automethod:: Postgres.sql
