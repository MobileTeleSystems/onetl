# Чтение из Greenplum с использованием `DBReader` { #greenplum-read }

Данные могут быть прочитаны из Greenplum в Spark с использованием [DBReader][db-reader]. Он также поддерживает [стратегии][strategy] для инкрементального чтения данных.

!!! warning

    Пожалуйста, учитывайте [типы данных Greenplum][greenplum-types].

!!! note

    В отличие от коннекторов JDBC, *Greenplum connector for Spark* не поддерживает выполнение **пользовательских** SQL-запросов с использованием метода `.sql`. Коннектор может использоваться только для чтения данных из таблицы или представления.

## Поддерживаемые функции DBReader

- ✅︎ `columns` (см. примечание ниже)
- ✅︎ `where` (см. примечание ниже)
- ✅︎ `hwm` (см. примечание ниже), поддерживаемые стратегии:
  - ✅︎ [Snapshot][snapshot-strategy]
  - ✅︎ [Incremental][incremental-strategy]
  - ✅︎ [Snapshot batch][snapshot-batch-strategy]
  - ✅︎ [Incremental batch][incremental-batch-strategy]
- ❌ `hint` (не поддерживается Greenplum)
- ❌ `df_schema`
- ✅︎ `options` (см. [Greenplum.ReadOptions][onetl.connection.db_connection.greenplum.options.GreenplumReadOptions])

!!! warning

    В случае коннектора Greenplum, `DBReader` не генерирует сырой запрос `SELECT`. Вместо этого он полагается на синтаксис Spark SQL, который в некоторых случаях (при использовании проекции столбцов и предикатов) может быть преобразован в SQL Greenplum.

    Поэтому `columns`, `where` и `hwm.expression` должны быть указаны в синтаксисе [Spark SQL](https://spark.apache.org/docs/latest/sql-ref-syntax.html), а не в SQL Greenplum.

    Это правильно:

    ```python

        DBReader(
            columns=[
                "some_column",
                # это преобразование выполняется на стороне Spark
                "CAST(another_column AS STRING)",
            ],
            # этот предикат анализируется Spark и может быть передан в Greenplum
            where="some_column LIKE 'val1%'",
        ) 
    ```

    А это приведет к ошибке:

    ```python

        DBReader(
            columns=[
                "some_column",
                # у Spark нет типа `text`
                "CAST(another_column AS text)",
            ],
            # Spark не поддерживает синтаксис ~ для сопоставления регулярных выражений
            where="some_column ~ 'val1.*'",
        ) 
    ```

## Примеры

Стратегия Snapshot:

    ```python
        from onetl.connection import Greenplum
        from onetl.db import DBReader

        greenplum = Greenplum(...)

        reader = DBReader(
            connection=greenplum,
            source="schema.table",
            columns=["id", "key", "CAST(value AS string) value", "updated_dt"],
            where="key = 'something'",
        )
        df = reader.run() 
        
    ```

Стратегия Incremental:

    ```python
        from onetl.connection import Greenplum
        from onetl.db import DBReader
        from onetl.strategy import IncrementalStrategy

        greenplum = Greenplum(...)

        reader = DBReader(
            connection=greenplum,
            source="schema.table",
            columns=["id", "key", "CAST(value AS string) value", "updated_dt"],
            where="key = 'something'",
            hwm=DBReader.AutoDetectHWM(name="greenplum_hwm", expression="updated_dt"),
        )

        with IncrementalStrategy():
            df = reader.run()
    ```

## Схема взаимодействия

Высокоуровневая схема описана в [Greenplum prerequisites][greenplum-prerequisites]. Ниже вы можете найти подробную схему взаимодействия.

??? note "Схема взаимодействия Spark <-> Greenplum во время DBReader.run()"

    ```plantuml

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
                note right of "Greenplum segment1" : No direct requests between Greenplum segments & Spark driver.\nData transfer is always initiated by Greenplum segments.

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
    ```

    ```mermaid
    ---
    title: Greenplum master <-> Spark driver
    ---

    sequenceDiagram
        box "Spark"
        participant A as "Spark driver"
        participant B as "Spark executor1"
        participant C as "Spark executor2"
        participant D as "Spark executorN"
        end
        
        box "Greenplum"
        participant E as "Greenplum master"
        participant F as "Greenplum segment1"
        participant G as "Greenplum segment2"
        participant H as "Greenplum segmentN"
        end
        
        note over A,H: == Greenplum.check() ==
        
        activate A
        activate E
        A ->> E: CONNECT
        
        A -->> E : CHECK IF TABLE EXISTS gp_table
        E -->> A : TABLE EXISTS
        A ->> E : SHOW SCHEMA FOR gp_table
        E -->> A : (id bigint, col1 int, col2 text, ...)
        
        note over A,H: == DBReader.run() ==
        
        A ->> B: START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION 1
        A ->> C: START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION 2
        A ->> D: START EXECUTOR FOR df(id bigint, col1 int, col2 text, ...) PARTITION N
        
        note right of A : This is done in parallel,<br/>executors are independent<br/>|<br/>|<br/>|<br/>V
        B ->> E: CREATE WRITABLE EXTERNAL TABLE spark_executor1 (id bigint, col1 int, col2 text, ...)<br/>USING address=executor1_host:executor1_port <br/>INSERT INTO EXTERNAL TABLE spark_executor1 FROM gp_table WHERE gp_segment_id = 1
        note right of E : Each white vertical line here is a opened connection to master.<br/>Usually, **N+1** connections are created from Spark to Greenplum master
        activate E
        E -->> F: SELECT DATA FROM gp_table_data_on_segment1 TO spark_executor1
        note right of F : No direct requests between Greenplum segments & Spark driver.<br/>Data transfer is always initiated by Greenplum segments.
        

        C ->> E: CREATE WRITABLE EXTERNAL TABLE spark_executor2 (id bigint, col1 int, col2 text, ...)<br/>USING address=executor2_host:executor2_port <br/>INSERT INTO EXTERNAL TABLE spark_executor2 FROM gp_table WHERE gp_segment_id = 2
        activate E
        E -->> G: SELECT DATA FROM gp_table_data_on_segment2 TO spark_executor2
        
        D ->> E: CREATE WRITABLE EXTERNAL TABLE spark_executorN (id bigint, col1 int, col2 text, ...)<br/>USING address=executorN_host:executorN_port <br/>INSERT INTO EXTERNAL TABLE spark_executorN FROM gp_table WHERE gp_segment_id = N
        activate E
        E -->> H: SELECT DATA FROM gp_table_data_on_segmentN TO spark_executorN
        
        F -xB: INITIALIZE CONNECTION TO Spark executor1<br/>PUSH DATA TO Spark executor1
        note left of B : Circle is an open GPFDIST port,<br/>listened by executor
        
        G -xC: INITIALIZE CONNECTION TO Spark executor2<br/>PUSH DATA TO Spark executor2
        H -xD: INITIALIZE CONNECTION TO Spark executorN<br/>PUSH DATA TO Spark executorN
        
        note over A,H: == Spark.stop() ==
        
        B -->> E : DROP TABLE spark_executor1
        deactivate E
        C -->> E : DROP TABLE spark_executor2
        deactivate E
        D -->> E : DROP TABLE spark_executorN
        deactivate E
        
        B -->> A: DONE
        C -->> A: DONE
        D -->> A: DONE
        
        A -->> E : CLOSE CONNECTION
        deactivate E
        deactivate A
    ```

## Рекомендации

### Выбирайте только необходимые столбцы

Вместо передачи `"*"` в `DBReader(columns=[...])` предпочтительнее передавать точные имена столбцов. Это уменьшает объем данных, передаваемых из Greenplum в Spark.

### Обратите внимание на значение `where`

Вместо фильтрации данных на стороне Spark с использованием `df.filter(df.column == 'value')` передайте правильное условие `DBReader(where="column = 'value'")`. Это уменьшает объем данных, отправляемых из Greenplum в Spark, и может улучшить производительность запроса. Особенно если есть индексы или разделы для столбцов, используемых в условии `where`.

### Параллельное чтение данных

`DBReader` в случае коннектора Greenplum требует, чтобы представление или таблица имели столбец, который используется Spark для параллельного чтения.

Выбор правильного столбца позволяет каждому исполнителю Spark читать только часть данных, хранящихся в указанном сегменте, избегая перемещения больших объемов данных между сегментами, что улучшает производительность чтения.

#### Использование `gp_segment_id`

По умолчанию `DBReader` будет использовать столбец [gp_segment_id](https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/troubleshooting.html#reading-from-a-view) для параллельного чтения данных. Каждый раздел DataFrame будет содержать данные конкретного сегмента Greenplum.

Это позволяет каждому исполнителю Spark читать только данные из определенного сегмента Greenplum, избегая перемещения больших объемов данных между сегментами.

Если используется представление, рекомендуется включить столбец `gp_segment_id` в это представление:

??? note "Чтение из представления со столбцом gp_segment_id"

    ```python

        from onetl.connection import Greenplum
        from onetl.db import DBReader

        greenplum = Greenplum(...)

        greenplum.execute(
            """
            CREATE VIEW schema.view_with_gp_segment_id AS
            SELECT
                id,
                some_column,
                another_column,
                gp_segment_id  -- ВАЖНО
            FROM schema.some_table
            """,
        )

        reader = DBReader(
            connection=greenplum,
            source="schema.view_with_gp_segment_id",
        )
        df = reader.run() 
    ```

#### Использование пользовательского `partition_column`

Иногда в таблице или представлении отсутствует столбец `gp_segment_id`, но есть некоторый столбец с диапазоном значений, коррелирующим с распределением сегмента Greenplum.

В этом случае вместо него можно использовать пользовательский столбец:

??? note "Чтение из представления с пользовательским partition_column"

    ```python

        from onetl.connection import Greenplum
        from onetl.db import DBReader

        greenplum = Greenplum(...)

        greenplum.execute(
            """
            CREATE VIEW schema.view_with_partition_column AS
            SELECT
                id,
                some_column,
                part_column  -- коррелирует с ID сегмента greenplum
            FROM schema.some_table
            """,
        )

        reader = DBReader(
            connection=greenplum,
            source="schema.view_with_partition_column",
            options=Greenplum.ReadOptions(
                # параллельное чтение данных с использованием указанного столбца
                partitionColumn="part_column",
                # создание 10 задач Spark, каждая будет читать только часть данных таблицы
                partitions=10,
            ),
        )
        df = reader.run() 
    ```

#### Чтение таблиц `DISTRIBUTED REPLICATED`

Реплицированные таблицы вообще не имеют столбца `gp_segment_id`, поэтому вам нужно установить `partition_column` на имя некоторого столбца типа integer/bigint/smallint.

### Параллельное выполнение `JOIN`

В случае использования представлений, которые требуют перемещения данных между сегментами Greenplum, например запросов `JOIN`, следует использовать другой подход.

Каждый экзекутор Spark из N будет выполнять один и тот же запрос, поэтому каждый из N запросов запустит свой собственный процесс JOIN, что приведет к очень высокой нагрузке на сегменты Greenplum.

**Этого следует избегать**.

Вместо этого рекомендуется выполнить запрос `JOIN` на стороне Greenplum, сохранить результат во временной таблице, а затем прочитать эту таблицу с помощью `DBReader`:

??? note "Чтение из представления с использованием промежуточной таблицы"

    ```python

        from onetl.connection import Greenplum
        from onetl.db import DBReader

        greenplum = Greenplum(...)

        greenplum.execute(
            """
            CREATE UNLOGGED TABLE schema.intermediate_table AS
            SELECT
                id,
                tbl1.col1,
                tbl1.data,
                tbl2.another_data
            FROM
                schema.table1 as tbl1
            JOIN
                schema.table2 as tbl2
            ON
                tbl1.col1 = tbl2.col2
            WHERE ...
            """,
        )

        reader = DBReader(
            connection=greenplum,
            source="schema.intermediate_table",
        )
        df = reader.run()

        # запись dataframe куда-либо

        greenplum.execute(
            """
            DROP TABLE schema.intermediate_table
            """,
        ) 
    ```

!!! warning

    **НИКОГДА** не делайте так:

    ```python

        df1 = DBReader(connection=greenplum, target="public.table1", ...).run()
        df2 = DBReader(connection=greenplum, target="public.table2", ...).run()

        joined_df = df1.join(df2, on="col") 
    ```

    Это приведет к отправке всех данных из обеих таблиц `table1` и `table2` в память исполнителя Spark, а затем `JOIN` будет выполнен на стороне Spark, а не внутри Greenplum. Это **ОЧЕНЬ** неэффективно.

#### Примечание о таблицах `TEMPORARY`

Кто-то может подумать, что запись данных из представления или результата `JOIN` во временную таблицу (`TEMPORARY`), а затем передача ее в `DBReader`, является эффективным способом чтения данных из Greenplum. Это потому, что временные таблицы не генерируют файлы WAL и автоматически удаляются после завершения транзакции.

Это **НЕ** сработает. Каждый экзекутор Spark устанавливает свое собственное соединение с Greenplum. И каждое соединение начинает свою собственную транзакцию, что означает, что каждый исполнитель будет читать пустую временную таблицу.

Вам следует использовать таблицы [UNLOGGED](https://docs.vmware.com/en/VMware-Greenplum/7/greenplum-database/ref_guide-sql_commands-CREATE_TABLE.html) для записи данных во временную таблицу без генерации журналов WAL.

## Параметры

### GreenplumReadOptions

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.greenplum.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: GreenplumReadOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```
 -->

::: onetl.connection.db_connection.greenplum.options.GreenplumReadOptions
    options:
        show_root_heading: true
        heading_level: 3
