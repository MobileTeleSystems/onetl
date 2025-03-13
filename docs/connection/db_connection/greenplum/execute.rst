.. _greenplum-execute:

Executing statements in Greenplum
==================================

.. warning::

    Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

    Do **NOT** use them to read large amounts of data. Use :ref:`DBReader <greenplum-read>` instead.

How to
------

There are 2 ways to execute some statement in Greenplum

Use ``Greenplum.fetch``
~~~~~~~~~~~~~~~~~~~~~~~

Use this method to perform some ``SELECT`` query which returns **small number or rows**, like reading
Greenplum config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts :obj:`Greenplum.FetchOptions <onetl.connection.db_connection.greenplum.options.GreenplumFetchOptions>`.

Connection opened using this method should be then closed with ``connection.close()`` or ``with connection:``.

.. warning::

    ``Greenplum.fetch`` is implemented using Postgres JDBC connection,
    so types are handled a bit differently than in ``DBReader``. See :ref:`postgres-types`.

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
        options=Greenplum.FetchOptions(queryTimeout=10),
    )
    greenplum.close()
    value = df.collect()[0][0]  # get value from first row and first column

Use ``Greenplum.execute``
~~~~~~~~~~~~~~~~~~~~~~~~~

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts :obj:`Greenplum.ExecuteOptions <onetl.connection.db_connection.greenplum.options.GreenplumExecuteOptions>`.

Connection opened using this method should be then closed with ``connection.close()`` or ``with connection:``.

Syntax support
^^^^^^^^^^^^^^

This method supports **any** query syntax supported by Greenplum, like:

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

    from onetl.connection import Greenplum

    greenplum = Greenplum(...)

    greenplum.execute("DROP TABLE schema.table")
    greenplum.execute(
        """
        CREATE TABLE schema.table (
            id int,
            key text,
            value real
        )
        DISTRIBUTED BY id
        """,
        options=Greenplum.ExecuteOptions(queryTimeout=10),
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

.. currentmodule:: onetl.connection.db_connection.greenplum.options

.. autopydantic_model:: GreenplumFetchOptions
    :inherited-members: GenericOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false


.. autopydantic_model:: GreenplumExecuteOptions
    :inherited-members: GenericOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
