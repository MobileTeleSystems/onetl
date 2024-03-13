.. _mysql-sql:

Reading from MySQL using ``MySQL.sql``
======================================

.. warning::

    Please take into account :ref:`mysql-types`

:obj:`MySQL.sql <onetl.connection.db_connection.mysql.connection.MySQL.sql>` allows passing custom SQL query,
but does not support incremental strategies.

Method also accepts :obj:`JDBCReadOptions <onetl.connection.db_connection.jdbc.options.JDBCReadOptions>`.

Syntax support
--------------

Only queries with the following syntax are supported:

* ✅︎ ``SELECT ... FROM ...``
* ✅︎ ``WITH alias AS (...) SELECT ...``
* ❌ ``SHOW ...``
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
--------

.. code-block:: python

    from onetl.connection import MySQL

    mysql = MySQL(...)
    df = mysql.sql(
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
        options=MySQL.ReadOptions(partition_column="id", num_partitions=10),
    )

References
----------

.. currentmodule:: onetl.connection.db_connection.mysql.connection

.. automethod:: MySQL.sql
