.. _greenplum-execute:

Executing statements in Greenplum
==================================

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
