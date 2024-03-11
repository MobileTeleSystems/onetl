.. _greenplum-execute:

Executing statements in Greenplum
==================================

.. warning::

    Unlike ``DBReader``, ``Greenplum.fetch`` and ``Greenplum.execute`` are implemented using Postgres JDBC connection,
    and so types are handled a bit differently. See :ref:`postgres-types`.

How to
------

There are 2 ways to execute some statement in Greenplum

Use :obj:`Greenplum.fetch <onetl.connection.db_connection.greenplum.connection.Greenplum.fetch>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute some ``SELECT`` query which returns **small number or rows**, like reading
Greenplum config, or reading data from some reference table.

Method accepts :obj:`JDBCOptions <onetl.connection.db_connection.jdbc_mixin.options.JDBCOptions>`.

Connection opened using this method should be then closed with :obj:`Greenplum.close <onetl.connection.db_connection.greenplum.connection.Greenplum.close>`.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Greenplum, like:

* ✅︎ ``SELECT ... FROM ...``
* ✅︎ ``WITH alias AS (...) SELECT ...``
* ✅︎ ``SELECT func(arg1, arg2)`` or ``{call func(arg1, arg2)}`` - special syntax for calling functions
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Greenplum

    greenplum = Greenplum(...)

    df = greenplum.fetch(
        "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
        options=Greenplum.JDBCOptions(query_timeout=10),
    )
    greenplum.close()
    value = df.collect()[0][0]  # get value from first row and first column

Use :obj:`Greenplum.execute <onetl.connection.db_connection.greenplum.connection.Greenplum.execute>`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts :obj:`JDBCOptions <onetl.connection.db_connection.jdbc_mixin.options.JDBCOptions>`.

Connection opened using this method should be then closed with :obj:`Greenplum.close <onetl.connection.db_connection.greenplum.connection.Greenplum.close>`.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Greenplum, like:

* ✅︎ ``CREATE TABLE ...``, ``CREATE VIEW ...``, and so on
* ✅︎ ``ALTER ...``
* ✅︎ ``INSERT INTO ... AS SELECT ...``
* ✅︎ ``DROP TABLE ...``, ``DROP VIEW ...``, and so on
* ✅︎ ``CALL procedure(arg1, arg2) ...``
* ✅︎ ``SELECT func(arg1, arg2)`` or ``{call func(arg1, arg2)}`` - special syntax for calling functions
* ✅︎ other statements not mentioned here
* ❌ ``SET ...; SELECT ...;`` - multiple statements not supported

Examples
^^^^^^^^

.. code-block:: python

    from onetl.connection import Greenplum

    greenplum = Greenplum(...)

    with greenplum:
        greenplum.execute("DROP TABLE schema.table")
        greenplum.execute(
            """
            CREATE TABLE schema.table AS (
                id int,
                key text,
                value real
            )
            DISTRIBUTED BY id
            """,
            options=Greenplum.JDBCOptions(query_timeout=10),
        )

Interaction schema
------------------

Unlike reading & writing, executing statements in Greenplum is done **only** through Greenplum master node,
without any interaction between Greenplum segments and Spark executors. More than that, Spark executors are not used in this case.

The only port used while interacting with Greenplum in this case is ``5432`` (Greenplum master port).

.. dropdown:: Spark <-> Greenplum interaction during Greenplum.execute()/Greenplum.fetch()

    .. plantuml::

        @startuml
        title Greenplum master <-> Spark driver
                box "Spark"
                participant "Spark driver"
                end box

                box "Greenplum"
                participant "Greenplum master"
                end box

                == Greenplum.check() ==

                activate "Spark driver"
                "Spark driver" -> "Greenplum master" ++ : CONNECT

                == Greenplum.execute(statement) ==
                "Spark driver" --> "Greenplum master" : EXECUTE statement
                "Greenplum master" -> "Spark driver" : RETURN result

                == Greenplum.close() ==
                "Spark driver" --> "Greenplum master" : CLOSE CONNECTION

                deactivate "Greenplum master"
                deactivate "Spark driver"
        @enduml

Options
-------

.. currentmodule:: onetl.connection.db_connection.greenplum.connection

.. automethod:: Greenplum.fetch
.. automethod:: Greenplum.execute
.. automethod:: Greenplum.close

.. currentmodule:: onetl.connection.db_connection.jdbc_mixin.options

.. autopydantic_model:: JDBCOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
