.. _greenplum-write:

Writing to Greenplum
=====================

For writing data to Greenplum, use :obj:`DBWriter <onetl.db.db_writer.db_writer.DBWriter>` with options below.


Interaction schema
------------------

High-level schema is described in :ref:`greenplum-prerequisites`. You can find detailed interaction schema below.

.. dropdown:: Spark <-> Greenplum interaction during DBWriter.run()

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
                "Spark driver" --> "Greenplum master" ++ : CHECK IF TABLE EXISTS gp_table
                "Greenplum master" --> "Spark driver" : TABLE NOT EXISTS

                == DBWriter.run(df) ==

                "Spark driver" -> "Spark executor1" ++ : START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION 1
                "Spark driver" -> "Spark executor2" ++ : START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION 2
                "Spark driver" -> "Spark executorN" ++ : START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION N

                note right of "Spark driver" : This is done in parallel,\nexecutors are independent\n|\n|\n|\nV
                "Spark executor1" -> "Greenplum master" ++ : CREATE READABLE EXTERNAL TABLE spark_executor1 (id bigint, col1 int, col2 text, ...) USING address=executor1_host:executor1_port;\nINSERT INTO gp_table FROM spark_executor1
                note right of "Greenplum master" : Each white vertical line here is a opened connection to master.\nUsually, **N+1** connections are created from Spark to Greenplum master
                "Greenplum master" --> "Greenplum segment1" ++ : SELECT DATA FROM spark_executor1 TO gp_table_data_on_segment1
                note right of "Greenplum segment1" : No direct requests between Greenplum segments & Spark.\nData transfer is always initiated by Greenplum segments.

                "Spark executor2" -> "Greenplum master" ++ : CREATE READABLE EXTERNAL TABLE spark_executor2 (id bigint, col1 int, col2 text, ...) USING address=executor2_host:executor2_port;\nINSERT INTO gp_table FROM spark_executor2
                "Greenplum master" --> "Greenplum segment2" ++ : SELECT DATA FROM spark_executor2 TO gp_table_data_on_segment2

                "Spark executorN" -> "Greenplum master" ++ : CREATE READABLE EXTERNAL TABLE spark_executorN (id bigint, col1 int, col2 text, ...) USING address=executorN_host:executorN_port;\nINSERT INTO gp_table FROM spark_executorN
                "Greenplum master" --> "Greenplum segmentN" ++ : SELECT DATA FROM spark_executorN TO gp_table_data_on_segmentN

                "Greenplum segment1" -->o "Spark executor1" : INITIALIZE CONNECTION TO Spark executor1
                "Spark executor1" -> "Greenplum segment1" : READ DATA FROM Spark executor1
                note left of "Spark executor1" : Circle is an open GPFDIST port,\nlistened by executor
                deactivate "Greenplum segment1"

                "Greenplum segment2" -->o "Spark executor2" : INITIALIZE CONNECTION TO Spark executor2
                "Spark executor2" -> "Greenplum segment2" : READ DATA FROM Spark executor2
                deactivate "Greenplum segment2"

                "Greenplum segmentN" -->o "Spark executorN" : INITIALIZE CONNECTION TO Spark executorN
                "Spark executorN" -> "Greenplum segmentN" : READ DATA FROM Spark executorN
                deactivate "Greenplum segmentN"

                == Finished ==

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

Mapping of Spark types to Greenplum types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

See `official documentation <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.1/greenplum-connector-spark/reference-datatype_mapping.html#spark-to-greenplum>`_
for more details.
onETL does not perform any additional casting of types while writing data.

Options
-------

.. currentmodule:: onetl.connection.db_connection.greenplum.options

.. autopydantic_model:: GreenplumWriteOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
