.. _mysql-execute:

Executing statements in MySQL
==================================

How to
------

There are 2 ways to execute some statement in MySQL

Use :obj:`MySQL.fetch <onetl.connection.db_connection.mysql.connection.MySQL.fetch>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute some ``SELECT`` query which returns **small number or rows**, like reading
MySQL config, or reading data from some reference table.

Method accepts :obj:`JDBCOptions <onetl.connection.db_connection.jdbc_mixin.options.JDBCOptions>`.

Connection opened using this method should be then closed with :obj:`MySQL.close <onetl.connection.db_connection.mysql.connection.MySQL.close>`.

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
        options=MySQL.JDBCOptions(query_timeout=10),
    )
    mysql.close()
    value = df.collect()[0][0]  # get value from first row and first column

Use :obj:`MySQL.execute <onetl.connection.db_connection.mysql.connection.MySQL.execute>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts :obj:`JDBCOptions <onetl.connection.db_connection.jdbc_mixin.options.JDBCOptions>`.

Connection opened using this method should be then closed with :obj:`MySQL.close <onetl.connection.db_connection.mysql.connection.MySQL.close>`.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by MySQL, like:

* ✅︎ ``CREATE TABLE ...``, ``CREATE VIEW ...``, and so on
* ✅︎ ``ALTER ...``
* ✅︎ ``INSERT INTO ... AS SELECT ...``
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
            options=MySQL.JDBCOptions(query_timeout=10),
        )

References
----------

.. currentmodule:: onetl.connection.db_connection.mysql.connection

.. automethod:: MySQL.fetch
.. automethod:: MySQL.execute
.. automethod:: MySQL.close

.. currentmodule:: onetl.connection.db_connection.jdbc_mixin.options

.. autopydantic_model:: JDBCOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
