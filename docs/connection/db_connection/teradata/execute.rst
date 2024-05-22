.. _teradata-execute:

Executing statements in Teradata
================================

.. warning::

    Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

    Do **NOT** use them to read large amounts of data. Use :ref:`DBReader <teradata-read>` or :ref:`Teradata.sql <teradata-sql>` instead.

How to
------

There are 2 ways to execute some statement in Teradata

Use ``Teradata.fetch``
~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute some ``SELECT`` query which returns **small number or rows**, like reading
Teradata config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts :obj:`Teradata.FetchOptions <onetl.connection.db_connection.teradata.options.TeradataFetchOptions>`.

Connection opened using this method should be then closed with ``connection.close()`` or ``with connection:``.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Teradata, like:

* ✅︎ ``SELECT ... FROM ...``
* ✅︎ ``WITH alias AS (...) SELECT ...``
* ✅︎ ``SHOW ...``
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Teradata

    teradata = Teradata(...)

    df = teradata.fetch(
        "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
        options=Teradata.FetchOptions(query_timeout=10),
    )
    teradata.close()
    value = df.collect()[0][0]  # get value from first row and first column

Use ``Teradata.execute``
~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts :obj:`Teradata.ExecuteOptions <onetl.connection.db_connection.teradata.options.TeradataExecuteOptions>`.

Connection opened using this method should be then closed with ``connection.close()`` or ``with connection:``.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Teradata, like:

* ✅︎ ``CREATE TABLE ...``, ``CREATE VIEW ...``, ``DROP TABLE ...``, and so on
* ✅︎ ``ALTER ...``
* ✅︎ ``INSERT INTO ... SELECT ...``, ``UPDATE ...``, ``DELETE ...``, and so on
* ✅︎ ``DROP TABLE ...``, ``DROP VIEW ...``, and so on
* ✅︎ ``CALL procedure(arg1, arg2) ...`` or ``{call procedure(arg1, arg2)}`` - special syntax for calling procedure
* ✅︎ ``EXECUTE macro(arg1, arg2)``
* ✅︎ ``EXECUTE FUNCTION ...``
* ✅︎ other statements not mentioned here
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Teradata

    teradata = Teradata(...)

    with teradata:
        # automatically close connection after exiting this context manager
        teradata.execute("DROP TABLE database.table")
        teradata.execute(
            """
            CREATE MULTISET TABLE database.table AS (
                id BIGINT,
                key VARCHAR,
                value REAL
            )
            NO PRIMARY INDEX
            """,
            options=Teradata.ExecuteOptions(query_timeout=10),
        )

Options
-------

.. currentmodule:: onetl.connection.db_connection.teradata.options

.. autopydantic_model:: TeradataFetchOptions
    :inherited-members: GenericOptions
    :member-order: bysource


.. autopydantic_model:: TeradataExecuteOptions
    :inherited-members: GenericOptions
    :member-order: bysource
