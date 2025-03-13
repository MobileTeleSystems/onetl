.. _postgres-execute:

Executing statements in Postgres
================================

.. warning::

    Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

    Do **NOT** use them to read large amounts of data. Use :ref:`DBReader <postgres-read>` or :ref:`Postgres.sql <postgres-sql>` instead.

How to
------

There are 2 ways to execute some statement in Postgres

Use ``Postgres.fetch``
~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute some ``SELECT`` query which returns **small number or rows**, like reading
Postgres config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts :obj:`Postgres.FetchOptions <onetl.connection.db_connection.postgres.options.PostgresFetchOptions>`.

Connection opened using this method should be then closed with ``connection.close()`` or ``with connection:``.

.. warning::

    Please take into account :ref:`postgres-types`.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Postgres, like:

* ✅︎ ``SELECT ... FROM ...``
* ✅︎ ``WITH alias AS (...) SELECT ...``\
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Postgres

    postgres = Postgres(...)

    df = postgres.fetch(
        "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
        options=Postgres.FetchOptions(queryTimeout=10),
    )
    postgres.close()
    value = df.collect()[0][0]  # get value from first row and first column

Use ``Postgres.execute``
~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts :obj:`Postgres.ExecuteOptions <onetl.connection.db_connection.postgres.options.PostgresExecuteOptions>`.

Connection opened using this method should be then closed with ``connection.close()`` or ``with connection:``.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Postgres, like:

* ✅︎ ``CREATE TABLE ...``, ``CREATE VIEW ...``, and so on
* ✅︎ ``ALTER ...``
* ✅︎ ``INSERT INTO ... SELECT ...``, ``UPDATE ...``, ``DELETE ...``, and so on
* ✅︎ ``DROP TABLE ...``, ``DROP VIEW ...``, ``TRUNCATE TABLE``, and so on
* ✅︎ ``CALL procedure(arg1, arg2) ...``
* ✅︎ ``SELECT func(arg1, arg2)`` or ``{call func(arg1, arg2)}`` - special syntax for calling functions
* ✅︎ other statements not mentioned here
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Postgres

    postgres = Postgres(...)

    postgres.execute("DROP TABLE schema.table")
    postgres.execute(
        """
        CREATE TABLE schema.table (
            id bigint GENERATED ALWAYS AS IDENTITY,
            key text,
            value real
        )
        """,
        options=Postgres.ExecuteOptions(queryTimeout=10),
    )

Options
-------

.. currentmodule:: onetl.connection.db_connection.postgres.options

.. autopydantic_model:: PostgresFetchOptions
    :inherited-members: GenericOptions
    :member-order: bysource


.. autopydantic_model:: PostgresExecuteOptions
    :inherited-members: GenericOptions
    :member-order: bysource
