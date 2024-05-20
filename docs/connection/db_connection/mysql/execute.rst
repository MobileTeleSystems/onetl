.. _mysql-execute:

Executing statements in MySQL
=============================

.. warning::

    Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

    Do **NOT** use them to read large amounts of data. Use :ref:`DBReader <mysql-read>` or :ref:`MySQL.sql <mysql-sql>` instead.

How to
------

There are 2 ways to execute some statement in MySQL

Use ``MySQL.fetch``
~~~~~~~~~~~~~~~~~~~

Use this method to perform some ``SELECT`` query which returns **small number or rows**, like reading
MySQL config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts :obj:`FetchOptions <onetl.connection.db_connection.jdbc_mixin.options.JDBCFetchOptions>`.

Connection opened using this method should be then closed with ``connection.close()`` or ``with connection:``.

.. warning::

    Please take into account :ref:`mysql-types`.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by MySQL, like:

* ✅︎ ``SELECT ... FROM ...``
* ✅︎ ``WITH alias AS (...) SELECT ...``
* ✅︎ ``SELECT func(arg1, arg2)`` or ``{?= call func(arg1, arg2)}`` - special syntax for calling function
* ✅︎ ``SHOW ...``
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import MySQL

    mysql = MySQL(...)

    df = mysql.fetch(
        "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
        options=MySQL.FetchOptions(query_timeout=10),
    )
    mysql.close()
    value = df.collect()[0][0]  # get value from first row and first column

Use ``MySQL.execute``
~~~~~~~~~~~~~~~~~~~~~

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts :obj:`ExecuteOptions <onetl.connection.db_connection.jdbc_mixin.options.JDBCExecuteOptions>`.

Connection opened using this method should be then closed with ``connection.close()`` or ``with connection:``.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by MySQL, like:

* ✅︎ ``CREATE TABLE ...``, ``CREATE VIEW ...``, ``DROP TABLE ...``, and so on
* ✅︎ ``ALTER ...``
* ✅︎ ``INSERT INTO ... SELECT ...``, ``UPDATE ...``, ``DELETE ...``, and so on
* ✅︎ ``DROP TABLE ...``, ``DROP VIEW ...``, and so on
* ✅︎ ``CALL procedure(arg1, arg2) ...`` or ``{call procedure(arg1, arg2)}`` - special syntax for calling procedure
* ✅︎ other statements not mentioned here
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import MySQL

    mysql = MySQL(...)

    with mysql:
        # automatically close connection after exiting this context manager
        mysql.execute("DROP TABLE schema.table")
        mysql.execute(
            """
            CREATE TABLE schema.table AS (
                id bigint,
                key text,
                value float
            )
            ENGINE = InnoDB
            """,
            options=MySQL.ExecuteOptions(query_timeout=10),
        )

Options
-------

.. currentmodule:: onetl.connection.db_connection.jdbc_mixin.options

.. autopydantic_model:: JDBCFetchOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false


.. autopydantic_model:: JDBCExecuteOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
