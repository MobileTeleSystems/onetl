# Чтение из Kafka { #kafka-read }

Данные могут быть прочитаны из Kafka в Spark с использованием [DBReader][db-reader].
Также поддерживается [стратегия][strategy] для инкрементального чтения данных.

## Поддерживаемые функции DBReader

- ❌ `columns` (не поддерживается Kafka)
- ❌ `where` (не поддерживается Kafka)
- ✅︎ `hwm`, поддерживаемые стратегии:
  - ✅︎ [Snapshot][snapshot-strategy]
  - ✅︎ [Incremental][incremental-strategy]
  - ❌ [Snapshot batch][snapshot-batch-strategy]
  - ❌ [Incremental batch][incremental-batch-strategy]
- ❌ `hint` (не поддерживается Kafka)
- ❌ `df_schema` (см. примечание ниже)
- ✅︎ `options` (см. [Kafka.ReadOptions][onetl.connection.db_connection.kafka.options.KafkaReadOptions])

## Схема DataFrame

В отличие от других подключений к БД, Kafka не имеет концепции столбцов.
Все сообщения топиков имеют одинаковый набор полей, структура представлена ниже:

    ```text
        root
        |-- key: binary (nullable = true)
        |-- value: binary (nullable = true)
        |-- topic: string (nullable = false)
        |-- partition: integer (nullable = false)
        |-- offset: integer (nullable = false)
        |-- timestamp: timestamp (nullable = false)
        |-- timestampType: integer (nullable = false)
        |-- headers: struct (nullable = true)
            |-- key: string (nullable = false)
            |-- value: binary (nullable = true)
    ```

Поле `headers` присутствует в DataFrame только если передан параметр `Kafka.ReadOptions(include_headers=True)` (совместимость с Kafka 1.x).

## Десериализация значений

Чтобы прочитать `value` или `key` другого типа, кроме байтов (например, структуру или целое число), пользователи должны десериализовать значения вручную.

Это можно сделать следующими методами:

- [Avro.parse_column][onetl.file.format.avro.Avro.parse_column]
- [JSON.parse_column][onetl.file.format.json.JSON.parse_column]
- [CSV.parse_column][onetl.file.format.csv.CSV.parse_column]
- [XML.parse_column][onetl.file.format.xml.XML.parse_column]

## Примеры

Стратегия Snapshot, `value` - двоичные данные Avro:

    ```python
        from onetl.connection import Kafka
        from onetl.db import DBReader, DBWriter
        from onetl.file.format import Avro
        from pyspark.sql.functions import decode

        # read all topic data from Kafka
        kafka = Kafka(...)
        reader = DBReader(connection=kafka, source="avro_topic")
        read_df = reader.run()

        # parse Avro format to Spark struct
        avro = Avro(
            schema_dict={
                "type": "record",
                "name": "Person",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                ],
            }
        )
        deserialized_df = read_df.select(
            # cast binary key to string
            decode("key", "UTF-8").alias("key"),
            avro.parse_column("value"),
        ) 
    ```

Инкрементальная стратегия, `value` - строка JSON:

!!! note

    В настоящее время коннектор Kafka поддерживает только HWM на основе поля `offset`. Другие поля, такие как `timestamp`, пока не поддерживаются.

    ```python
        from onetl.connection import Kafka
        from onetl.db import DBReader, DBWriter
        from onetl.file.format import JSON
        from pyspark.sql.functions import decode

        kafka = Kafka(...)

        # read only new data from Kafka topic
        reader = DBReader(
            connection=kafka,
            source="topic_name",
            hwm=DBReader.AutoDetectHWM(name="kafka_hwm", expression="offset"),
        )

        with IncrementalStrategy():
            read_df = reader.run()

        # parse JSON format to Spark struct
        json = JSON()
        schema = StructType(
            [
                StructField("name", StringType(), nullable=True),
                StructField("age", IntegerType(), nullable=True),
            ],
        )
        deserialized_df = read_df.select(
            # cast binary key to string
            decode("key", "UTF-8").alias("key"),
            json.parse_column("value", json),
        )
    ```

## Опции

::: onetl.connection.db_connection.kafka.options.KafkaReadOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
        extensions:
            - griffe_sphinx
            - griffe_pydantic: {schema: false}
