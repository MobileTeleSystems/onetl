.. _clickhouse-execute:

Executing statements in Clickhouse
==================================

.. warning::

    Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

    Do **NOT** use them to read large amounts of data. Use :ref:`DBReader <clickhouse-read>` or :ref:`Clickhouse.sql <clickhouse-sql>` instead.

How to
------

There are 2 ways to execute some statement in Clickhouse

Use ``Clickhouse.fetch``
~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute some ``SELECT`` query which returns **small number or rows**, like reading
Clickhouse config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts :obj:`JDBCOptions <onetl.connection.db_connection.jdbc_mixin.options.JDBCOptions>`.

Connection opened using this method should be then closed with ``connection.close()`` or ``with connection:``.

.. warning::

    Please take into account :ref:`clickhouse-types`.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Clickhouse, like:

* ✅︎ ``SELECT ... FROM ...``
* ✅︎ ``WITH alias AS (...) SELECT ...``
* ✅︎ ``SELECT func(arg1, arg2)`` - call function
* ✅︎ ``SHOW ...``
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

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

Use ``Clickhouse.execute``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts :obj:`JDBCOptions <onetl.connection.db_connection.jdbc_mixin.options.JDBCOptions>`.

Connection opened using this method should be then closed with ``connection.close()`` or ``with connection:``.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Clickhouse, like:

* ✅︎ ``CREATE TABLE ...``, ``CREATE VIEW ...``, ``DROP TABLE ...``, and so on
* ✅︎ ``ALTER ...``
* ✅︎ ``INSERT INTO ... SELECT ...``, ``UPDATE ...``, ``DELETE ...``, and so on
* ✅︎ ``DROP TABLE ...``, ``DROP VIEW ...``, and so on
* ✅︎ other statements not mentioned here
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Clickhouse

    clickhouse = Clickhouse(...)

    with clickhouse:
        # automatically close connection after exiting this context manager
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

Notes
------

These methods **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

So it should **NOT** be used to read large amounts of data. Use :ref:`DBReader <clickhouse-read>` or :ref:`Clickhouse.sql <clickhouse-sql>` instead.

Options
-------

.. currentmodule:: onetl.connection.db_connection.jdbc_mixin.options

.. autopydantic_model:: JDBCOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
