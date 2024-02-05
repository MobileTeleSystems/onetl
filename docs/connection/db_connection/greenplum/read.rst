.. _greenplum-read:

Reading from Greenplum
=======================

For reading data from Greenplum, use :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` with options below.

.. note::

    Unlike JDBC connectors, *Greenplum connector for Spark* does not support
    executing **custom** SQL queries using ``.sql`` method. Connector can be used to only read some table data
    (with filters, if needed) using DBReader.

Interaction schema
------------------

High-level schema is described in :ref:`greenplum-prerequisites`. You can find detailed interaction schema below.

.. dropdown:: Spark <-> Greenplum interaction during DBReader.run()

    .. plantuml::

        @startuml
        title Greenplum master <-> Spark driver
                box "Spark"
                participant "Spark driver"
                participant "Spark executor1"
                participant "Spark executor2"
                participant "Spark executorN"
                end box

                box "Greenplum"
                participant "Greenplum master"
                participant "Greenplum segment1"
                participant "Greenplum segment2"
                participant "Greenplum segmentN"
                end box

                == Greenplum.check() ==

                activate "Spark driver"
                "Spark driver" -> "Greenplum master" ++ : CONNECT

                "Spark driver" --> "Greenplum master" : CHECK IF TABLE EXISTS gp_table
                "Greenplum master" --> "Spark driver" : TABLE EXISTS
                "Spark driver" -> "Greenplum master" : SHOW SCHEMA FOR gp_table
                "Greenplum master" --> "Spark driver" : (id bigint, col1 int, col2 text, ...)

                == DBReader.run() ==

                "Spark driver" -> "Spark executor1" ++ : START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION 1
                "Spark driver" -> "Spark executor2" ++ : START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION 2
                "Spark driver" -> "Spark executorN" ++ : START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION N

                note right of "Spark driver" : This is done in parallel,\nexecutors are independent\n|\n|\n|\nV
                "Spark executor1" -> "Greenplum master" ++ : CREATE WRITABLE EXTERNAL TABLE spark_executor1 (id bigint, col1 int, col2 text, ...) USING address=executor1_host:executor1_port;\nINSERT INTO EXTERNAL TABLE spark_executor1 FROM gp_table WHERE gp_segment_id = 1
                note right of "Greenplum master" : Each white vertical line here is a opened connection to master.\nUsually, **N+1** connections are created from Spark to Greenplum master
                "Greenplum master" --> "Greenplum segment1" ++ : SELECT DATA FROM gp_table_data_on_segment1 TO spark_executor1
                note right of "Greenplum segment1" : No direct requests between Greenplum segments & Spark.\nData transfer is always initiated by Greenplum segments.

                "Spark executor2" -> "Greenplum master" ++ : CREATE WRITABLE EXTERNAL TABLE spark_executor2 (id bigint, col1 int, col2 text, ...) USING address=executor2_host:executor2_port;\nINSERT INTO EXTERNAL TABLE spark_executor2 FROM gp_table WHERE gp_segment_id = 2
                "Greenplum master" --> "Greenplum segment2" ++ : SELECT DATA FROM gp_table_data_on_segment2 TO spark_executor2

                "Spark executorN" -> "Greenplum master" ++ : CREATE WRITABLE EXTERNAL TABLE spark_executorN (id bigint, col1 int, col2 text, ...) USING address=executorN_host:executorN_port;\nINSERT INTO EXTERNAL TABLE spark_executorN FROM gp_table WHERE gp_segment_id = N
                "Greenplum master" --> "Greenplum segmentN" ++ : SELECT DATA FROM gp_table_data_on_segmentN TO spark_executorN

                "Greenplum segment1" ->o "Spark executor1" -- : INITIALIZE CONNECTION TO Spark executor1\nPUSH DATA TO Spark executor1
                note left of "Spark executor1" : Circle is an open GPFDIST port,\nlistened by executor

                "Greenplum segment2" ->o "Spark executor2" -- : INITIALIZE CONNECTION TO Spark executor2\nPUSH DATA TO Spark executor2
                "Greenplum segmentN" ->o "Spark executorN" -- : INITIALIZE CONNECTION TO Spark executorN\nPUSH DATA TO Spark executorN

                == Spark.stop() ==

                "Spark executor1" --> "Greenplum master" : DROP TABLE spark_executor1
                deactivate "Greenplum master"
                "Spark executor2" --> "Greenplum master" : DROP TABLE spark_executor2
                deactivate "Greenplum master"
                "Spark executorN" --> "Greenplum master" : DROP TABLE spark_executorN
                deactivate "Greenplum master"

                "Spark executor1" --> "Spark driver" -- : DONE
                "Spark executor2" --> "Spark driver" -- : DONE
                "Spark executorN" --> "Spark driver" -- : DONE

                "Spark driver" --> "Greenplum master" : CLOSE CONNECTION
                deactivate "Greenplum master"
                deactivate "Spark driver"
        @enduml

Recommendations
---------------

Reading from views
~~~~~~~~~~~~~~~~~~

This connector is **NOT** designed to read data from views.

You can technically read data from a view which has
`gp_segment_id <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/troubleshooting.html#reading-from-a-view>`_ column.
But this is **not** recommended because each Spark executor will run the same query, which may lead to running duplicated calculations
and sending data between segments only to skip most of the result and select only small part.

Prefer following option:
    * Create staging table to store result data, using :obj:`Greenplum.execute <onetl.connection.db_connection.greenplum.connection.Greenplum.execute>`
    * Use the same ``.execute`` method run a query ``INSERT INTO staging_table AS SELECT FROM some_view``. This will be done on Greenplum segments side, query will be run only once.
    * Read data from staging table to Spark executor using :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>`.
    * Drop staging table using ``.execute`` method.

Using ``JOIN`` on Greenplum side
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you need to get data of joining 2 tables in Greenplum, you should:
    * Create staging table to store result data, using ``Greenplum.execute``
    * Use the same ``Greenplum.execute`` run a query ``INSERT INTO staging_table AS SELECT FROM table1 JOIN table2``. This will be done on Greenplum segments side, in a distributed way.
    * Read data from staging table to Spark executor using ``DBReader``.
    * Drop staging table using ``Greenplum.execute``.

.. warning::

    Do **NOT** try to read data from ``table1`` and ``table2`` using ``DBReader``, and then join the resulting dataframes!

    This will lead to sending all the data from both tables to Spark executor memory, and then ``JOIN``
    will be performed on Spark side, not Greenplum. This is **very** inefficient.

Using ``TEMPORARY`` tables
~~~~~~~~~~~~~~~~~~~~~~~~~~

Someone could think that writing data from ``VIEW`` or result of ``JOIN`` to ``TEMPORARY`` table,
and then passing it to DBReader, is an efficient way to read data from Greenplum, because temp tables are not generating WAL files,
and are automatically deleted after finishing the transaction.

That's will **not** work. Each Spark executor establishes its own connection to Greenplum,
and thus reads its own temporary table, which does not contain any data.

You should use `UNLOGGED <https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-sql_commands-CREATE_TABLE.html>`_ tables
to write data to staging table without generating useless WAL logs.

Mapping of Greenplum types to Spark types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See `official documentation <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/reference-datatype_mapping.html#greenplum-to-spark>`_
for more details.
onETL does not perform any additional casting of types while reading data.

Options
-------

.. currentmodule:: onetl.connection.db_connection.greenplum.options

.. autopydantic_model:: GreenplumReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
