# Writing to Greenplum using `DBWriter` { #DBR-onetl-connection-db-connection-greenplum-write-writing-to-greenplum-using-dbwriter }

For writing data to Greenplum, use [DBWriter][DBR-onetl-db-writer] with [GreenplumWriteOptions][onetl.connection.db_connection.greenplum.options.GreenplumWriteOptions].

!!! warning

    Please take into account [Greenplum types][DBR-onetl-connection-db-connection-greenplum-types-greenplum-spark-type-mapping].

!!! warning

    It is always recommended to create table explicitly using [Greenplum.execute][DBR-onetl-connection-db-connection-greenplum-execute-executing-statements-in-greenplum]
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different types than it is expected.

## Examples { #DBR-onetl-connection-db-connection-greenplum-write-examples }

```python
from onetl.connection import Greenplum
from onetl.db import DBWriter

greenplum = Greenplum(...)

df = ...  # data is here

writer = DBWriter(
    connection=greenplum,
    target="schema.table",
    options=Greenplum.WriteOptions(
        if_exists="append",
        # by default distribution is random
        distributedBy="id",
        # partitionBy is not supported
    ),
)

writer.run(df)
```

## Interaction schema { #DBR-onetl-connection-db-connection-greenplum-write-interaction-schema }

High-level schema is described in [Greenplum prerequisites][DBR-onetl-connection-db-connection-greenplum-prerequisites]. You can find detailed interaction schema below.

??? note "Spark <-> Greenplum interaction during DBWriter.run()"

    ```mermaid
    ---
    title: Greenplum master ↔ Spark driver
    ---

    sequenceDiagram
        box Spark
        participant A as Spark driver
        participant B as Spark executor1
        participant C as Spark executor2
        participant D as Spark executorN
        end

        box Greenplum
        participant E as Greenplum master
        participant F as Greenplum segment1
        participant G as Greenplum segment2
        participant H as Greenplum segmentN
        end

        note over A,H: == Greenplum.check() ==
        A ->> E: CONNECT
        activate E
        activate A

        A -->> E : CHECK IF TABLE EXISTS gp_table
        E -->> A : TABLE EXISTS
        A ->> E : SHOW SCHEMA FOR gp_table
        E -->> A : (id bigint, col1 int, col2 text, ...)

        note over A,H: == DBReader.run() ==

        A ->> B: START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION 1
        activate B
        A ->> C: START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION 2
        activate C
        A ->> D: START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION N
        activate D

        note right of A : This is done in parallel,<br/>executors are independent<br/>|<br/>|<br/>|<br/>V

        B ->> E: CREATE WRITABLE EXTERNAL TABLE spark_executor1 (id bigint, col1 int, col2 text, ...)<br/>USING address=executor1_host:executor1_port<br/>INSERT INTO EXTERNAL TABLE spark_executor1 FROM gp_table WHERE gp_segment_id = 1
        activate E
        note right of E : Each white vertical line here is a opened connection to master.<br/>Usually, **N+1** connections are created from Spark to Greenplum master
        E -->> F: SELECT DATA FROM gp_table_data_on_segment1 TO spark_executor1
        activate F

        note right of F : No direct requests between Greenplum segments & Spark.<br/>Data transfer is always initiated by Greenplum segments.

        C ->> E: CREATE WRITABLE EXTERNAL TABLE spark_executor2 (id bigint, col1 int, col2 text, ...)<br/>USING address=executor2_host:executor2_port<br/>INSERT INTO EXTERNAL TABLE spark_executor2 FROM gp_table WHERE gp_segment_id = 2
        activate E
        E -->> G: SELECT DATA FROM gp_table_data_on_segment2 TO spark_executor2
        activate G

        D ->> E: CREATE WRITABLE EXTERNAL TABLE spark_executorN (id bigint, col1 int, col2 text, ...)<br/>USING address=executorN_host:executorN_port<br/>INSERT INTO EXTERNAL TABLE spark_executorN FROM gp_table WHERE gp_segment_id = N
        activate E
        E -->> H: SELECT DATA FROM gp_table_data_on_segmentN TO spark_executorN
        activate H


        F -xB: INITIALIZE CONNECTION TO Spark executor1<br/>PUSH DATA TO Spark executor1
        deactivate F
        note left of B : Circle is an open GPFDIST port,<br/>listened by executor

        G -xC: INITIALIZE CONNECTION TO Spark executor2<br/>PUSH DATA TO Spark executor2
        deactivate G
        H -xD: INITIALIZE CONNECTION TO Spark executorN<br/>PUSH DATA TO Spark executorN
        deactivate H

        note over A,H: == Spark.stop() ==

        B -->> E : DROP TABLE spark_executor1
        deactivate E
        C -->> E : DROP TABLE spark_executor2
        deactivate E
        D -->> E : DROP TABLE spark_executorN
        deactivate E

        B -->> A: DONE
        deactivate B
        C -->> A: DONE
        deactivate C
        D -->> A: DONE
        deactivate D

        A -->> E: CLOSE CONNECTION
        deactivate E
        deactivate A
    ```

## Options { #DBR-onetl-connection-db-connection-greenplum-write-options }


::: onetl.connection.db_connection.greenplum.options.GreenplumWriteOptions
