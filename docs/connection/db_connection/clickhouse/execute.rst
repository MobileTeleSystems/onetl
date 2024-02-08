.. _clickhouse-execute:

Executing statements in Clickhouse
==================================

How to
------

There are 2 ways to execute some statement in Clickhouse

Use :obj:`Clickhouse.fetch <onetl.connection.db_connection.clickhouse.connection.Clickhouse.fetch>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute some ``SELECT`` query which returns **small number or rows**, like reading
Clickhouse config, or reading data from some reference table.

Method accepts :obj:`JDBCOptions <onetl.connection.db_connection.jdbc_mixin.options.JDBCOptions>`.

Connection opened using this method should be then closed with :obj:`Clickhouse.close <onetl.connection.db_connection.clickhouse.connection.Clickhouse.close>`.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Clickhouse, like:

* ``SELECT ... FROM ...``
* ``WITH alias AS (...) SELECT ...``
* ``SHOW ...``

It does not support multiple queries in the same operation, like ``SET ...; SELECT ...;``.

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Clickhouse

    clickhouse = Clickhouse(...)

    df = clickhouse.fetch(
        "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
        options=Clickhouse.JDBCOptions(query_timeout=10),
    )
    clickhouse.close()
    value = df.collect()[0][0]  # get value from first row and first column

Use :obj:`Clickhouse.execute <onetl.connection.db_connection.clickhouse.connection.Clickhouse.execute>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts :obj:`JDBCOptions <onetl.connection.db_connection.jdbc_mixin.options.JDBCOptions>`.

Connection opened using this method should be then closed with :obj:`Clickhouse.close <onetl.connection.db_connection.clickhouse.connection.Clickhouse.close>`.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Clickhouse, like:

* ``CREATE TABLE ...``
* ``ALTER ...``
* ``INSERT INTO ... AS SELECT ...``

It does not support multiple queries in the same operation, like ``SET ...; CREATE TABLE ...;``.

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Clickhouse

    clickhouse = Clickhouse(...)

    with clickhouse:
        clickhouse.execute("DROP TABLE schema.table")
        clickhouse.execute(
            """
            CREATE TABLE schema.table AS (
                id UInt8,
                key String,
                value Float32
            )
            ENGINE = MergeTree()
            ORDER BY id
            """,
            options=Clickhouse.JDBCOptions(query_timeout=10),
        )


References
----------

.. currentmodule:: onetl.connection.db_connection.clickhouse.connection

.. automethod:: Clickhouse.fetch
.. automethod:: Clickhouse.execute
.. automethod:: Clickhouse.close

.. currentmodule:: onetl.connection.db_connection.jdbc_mixin.options

.. autopydantic_model:: JDBCOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
