.. _oracle-execute:

Executing statements in Oracle
==============================

.. warning::

    Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

    Do **NOT** use them to read large amounts of data. Use :ref:`DBReader <oracle-read>` or :ref:`Oracle.sql <oracle-sql>` instead.

How to
------

There are 2 ways to execute some statement in Oracle

Use ``Oracle.fetch``
~~~~~~~~~~~~~~~~~~~~

Use this method to execute some ``SELECT`` query which returns **small number or rows**, like reading
Oracle config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts :obj:`Oracle.FetchOptions <onetl.connection.db_connection.oracle.options.OracleFetchOptions>`.

Connection opened using this method should be then closed with ``connection.close()`` or ``with connection:``.

.. warning::

    Please take into account :ref:`oracle-types`.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Oracle, like:

* ✅︎ ``SELECT ... FROM ...``
* ✅︎ ``WITH alias AS (...) SELECT ...``
* ✅︎ ``SELECT func(arg1, arg2) FROM DUAL`` - call function
* ✅︎ ``SHOW ...``
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Oracle

    oracle = Oracle(...)

    df = oracle.fetch(
        "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
        options=Oracle.FetchOptions(query_timeout=10),
    )
    oracle.close()
    value = df.collect()[0][0]  # get value from first row and first column

Use ``Oracle.execute``
~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts :obj:`Oracle.ExecuteOptions <onetl.connection.db_connection.oracle.options.OracleExecuteOptions>`.

Connection opened using this method should be then closed with ``connection.close()`` or ``with connection:``.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Oracle, like:

* ✅︎ ``CREATE TABLE ...``, ``CREATE VIEW ...``
* ✅︎ ``ALTER ...``
* ✅︎ ``INSERT INTO ... SELECT ...``, ``UPDATE ...``, ``DELETE ...``, and so on
* ✅︎ ``DROP TABLE ...``, ``DROP VIEW ...``, and so on
* ✅︎ ``CALL procedure(arg1, arg2) ...`` or ``{call procedure(arg1, arg2)}`` - special syntax for calling procedure
* ✅︎ ``DECLARE ... BEGIN ... END`` - execute PL/SQL statement
* ✅︎ other statements not mentioned here
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Oracle

    oracle = Oracle(...)

    with oracle:
        # automatically close connection after exiting this context manager
        oracle.execute("DROP TABLE schema.table")
        oracle.execute(
            """
            CREATE TABLE schema.table AS (
                id bigint GENERATED ALWAYS AS IDENTITY,
                key VARCHAR2(4000),
                value NUMBER
            )
            """,
            options=Oracle.ExecuteOptions(query_timeout=10),
        )

Options
-------

.. currentmodule:: onetl.connection.db_connection.oracle.options

.. autopydantic_model:: OracleFetchOptions
    :inherited-members: GenericOptions
    :member-order: bysource


.. autopydantic_model:: OracleExecuteOptions
    :inherited-members: GenericOptions
    :member-order: bysource
