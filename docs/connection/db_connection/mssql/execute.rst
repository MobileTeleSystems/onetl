.. _mssql-execute:

Executing statements in MSSQL
=============================

.. warning::

    Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

    Do **NOT** use them to read large amounts of data. Use :ref:`DBReader <mssql-read>` or :ref:`MSSQL.sql <mssql-sql>` instead.

How to
------

There are 2 ways to execute some statement in MSSQL

Use ``MSSQL.fetch``
~~~~~~~~~~~~~~~~~~~

Use this method to perform some ``SELECT`` query which returns **small number or rows**, like reading
MSSQL config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts :obj:`MSSQL.FetchOptions <onetl.connection.db_connection.mssql.options.MSSQLFetchOptions>`.

Connection opened using this method should be then closed with ``connection.close()`` or ``with connection:``.

.. warning::

    Please take into account :ref:`mssql-types`.

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
        options=MSSQL.FetchOptions(query_timeout=10),
    )
    mssql.close()
    value = df.collect()[0][0]  # get value from first row and first column

Use ``MSSQL.execute``
~~~~~~~~~~~~~~~~~~~~~

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts :obj:`MSSQL.ExecuteOptions <onetl.connection.db_connection.mssql.options.MSSQLExecuteOptions>`.

Connection opened using this method should be then closed with ``connection.close()`` or ``with connection:``.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by MSSQL, like:

* ✅︎ ``CREATE TABLE ...``, ``CREATE VIEW ...``
* ✅︎ ``ALTER ...``
* ✅︎ ``INSERT INTO ... AS SELECT ...``
* ✅︎ ``DROP TABLE ...``, ``DROP VIEW ...``, and so on
* ✅︎ ``EXEC procedure(arg1, arg2) ...`` or ``{call procedure(arg1, arg2)}`` - special syntax for calling procedure
* ✅︎ ``DECLARE ... BEGIN ... END`` - execute PL/SQL statement
* ✅︎ other statements not mentioned here
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import MSSQL

    mssql = MSSQL(...)

    with mssql:
        # automatically close connection after exiting this context manager
        mssql.execute("DROP TABLE schema.table")
        mssql.execute(
            """
            CREATE TABLE schema.table (
                id bigint GENERATED ALWAYS AS IDENTITY,
                key VARCHAR2(4000),
                value NUMBER
            )
            """,
            options=MSSQL.ExecuteOptions(query_timeout=10),
        )

Options
-------

.. currentmodule:: onetl.connection.db_connection.mssql.options

.. autopydantic_model:: MSSQLFetchOptions
    :inherited-members: GenericOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false

.. autopydantic_model:: MSSQLExecuteOptions
    :inherited-members: GenericOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
