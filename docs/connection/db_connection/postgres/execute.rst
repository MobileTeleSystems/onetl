.. _postgres-execute:

Executing statements in Postgres
==================================

How to
------

There are 2 ways to execute some statement in Postgres

Use :obj:`Postgres.fetch <onetl.connection.db_connection.postgres.connection.Postgres.fetch>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute some ``SELECT`` query which returns **small number or rows**, like reading
Postgres config, or reading data from some reference table.

Method accepts :obj:`JDBCOptions <onetl.connection.db_connection.jdbc_mixin.options.JDBCOptions>`.

Connection opened using this method should be then closed with :obj:`Postgres.close <onetl.connection.db_connection.postgres.connection.Postgres.close>`.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Postgres, like:

* ``SELECT ... FROM ...``
* ``WITH alias AS (...) SELECT ...``

Queries like ``SHOW ...`` are not supported.

It does not support multiple queries in the same operation, like ``SET ...; SELECT ...;``.

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Postgres

    postgres = Postgres(...)

    df = postgres.fetch(
        "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
        options=Postgres.JDBCOptions(query_timeout=10),
    )
    postgres.close()
    value = df.collect()[0][0]  # get value from first row and first column

Use :obj:`Postgres.execute <onetl.connection.db_connection.postgres.connection.Postgres.execute>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts :obj:`JDBCOptions <onetl.connection.db_connection.jdbc_mixin.options.JDBCOptions>`.

Connection opened using this method should be then closed with :obj:`Postgres.close <onetl.connection.db_connection.postgres.connection.Postgres.close>`.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Postgres, like:

* ``CREATE TABLE ...``, ``CREATE VIEW ...``
* ``ALTER ...``
* ``INSERT INTO ... AS SELECT ...``
* ``DROP TABLE ...``, ``DROP VIEW ...``, and so on
* ``CALL procedure(arg1, arg2) ...``
* ``SELECT func(arg1, arg2)`` or ``{call func(arg1, arg2)}`` - special syntax for calling functions
* etc

It does not support multiple queries in the same operation, like ``SET ...; CREATE TABLE ...;``.

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Postgres

    postgres = Postgres(...)

    with postgres:
        postgres.execute("DROP TABLE schema.table")
        postgres.execute(
            """
            CREATE TABLE schema.table AS (
                id biging ALWAYS GENERATED AS IDENTITY,
                key text,
                value real
            )
            """,
            options=Postgres.JDBCOptions(query_timeout=10),
        )


References
----------

.. currentmodule:: onetl.connection.db_connection.postgres.connection

.. automethod:: Postgres.fetch
.. automethod:: Postgres.execute
.. automethod:: Postgres.close

.. currentmodule:: onetl.connection.db_connection.jdbc_mixin.options

.. autopydantic_model:: JDBCOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
