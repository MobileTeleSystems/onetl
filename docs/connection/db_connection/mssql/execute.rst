.. _mssql-execute:

Executing statements in MSSQL
=============================

How to
------

There are 2 ways to execute some statement in MSSQL

Use :obj:`MSSQL.fetch <onetl.connection.db_connection.mssql.connection.MSSQL.fetch>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute some ``SELECT`` query which returns **small number or rows**, like reading
MSSQL config, or reading data from some reference table.

Method accepts :obj:`JDBCOptions <onetl.connection.db_connection.jdbc_mixin.options.JDBCOptions>`.

Connection opened using this method should be then closed with :obj:`MSSQL.close <onetl.connection.db_connection.mssql.connection.MSSQL.close>`.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by MSSQL, like:

* ✅︎ ``SELECT ... FROM ...``
* ✅︎ ``WITH alias AS (...) SELECT ...``
* ✅︎ ``SELECT func(arg1, arg2) FROM DUAL`` - call function
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import MSSQL

    mssql = MSSQL(...)

    df = mssql.fetch(
        "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
        options=MSSQL.JDBCOptions(query_timeout=10),
    )
    mssql.close()
    value = df.collect()[0][0]  # get value from first row and first column

Use :obj:`MSSQL.execute <onetl.connection.db_connection.mssql.connection.MSSQL.execute>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts :obj:`JDBCOptions <onetl.connection.db_connection.jdbc_mixin.options.JDBCOptions>`.

Connection opened using this method should be then closed with :obj:`MSSQL.close <onetl.connection.db_connection.mssql.connection.MSSQL.close>`.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by MSSQL, like:

* ✅︎ ``CREATE TABLE ...``, ``CREATE VIEW ...``
* ✅︎ ``ALTER ...``
* ✅︎ ``INSERT INTO ... AS SELECT ...``
* ✅︎ ``DROP TABLE ...``, ``DROP VIEW ...``, and so on
* ✅︎ ``CALL procedure(arg1, arg2) ...`` or ``{call procedure(arg1, arg2)}`` - special syntax for calling procedure
* ✅︎ ``DECLARE ... BEGIN ... END`` - execute PL/SQL statement
* ✅︎ other statements not mentioned here
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import MSSQL

    mssql = MSSQL(...)

    with mssql:
        mssql.execute("DROP TABLE schema.table")
        mssql.execute(
            """
            CREATE TABLE schema.table AS (
                id bigint GENERATED ALWAYS AS IDENTITY,
                key VARCHAR2(4000),
                value NUMBER
            )
            """,
            options=MSSQL.JDBCOptions(query_timeout=10),
        )


References
----------

.. currentmodule:: onetl.connection.db_connection.mssql.connection

.. automethod:: MSSQL.fetch
.. automethod:: MSSQL.execute
.. automethod:: MSSQL.close

.. currentmodule:: onetl.connection.db_connection.jdbc_mixin.options

.. autopydantic_model:: JDBCOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
