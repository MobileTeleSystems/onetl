# Запись в Kafka { #kafka-write }

Для записи данных в Kafka используйте [DBWriter][db-writer] с определенными параметрами (см. ниже).

## Схема DataFrame

В отличие от других подключений к БД, Kafka не имеет концепции столбцов.
Все сообщения в топиках имеют одинаковый набор полей. Записать можно только некоторые из них:

    ```text
        root
        |-- key: binary (nullable = true)
        |-- value: binary (nullable = true)
        |-- headers: struct (nullable = true)
            |-- key: string (nullable = false)
            |-- value: binary (nullable = true)
    ```

Поле `headers` может быть передано только с `Kafka.WriteOptions(include_headers=True)` (для совместимости с Kafka 1.x).

Поле `topic` не должно присутствовать в DataFrame, так как оно передается в `DBWriter(target=...)`.

Другие поля, такие как `partition`, `offset`, `timestamp`, устанавливаются Kafka и не могут быть явно переданы.

## Сериализация значений

Для записи поля `value` или `key` типа, отличного от байтов (например, структуры или целого числа), пользователи должны сериализовать значения вручную.

Это можно сделать с помощью следующих методов:

- [Avro.serialize_column][onetl.file.format.avro.Avro.serialize_column]
- [JSON.serialize_column][onetl.file.format.json.JSON.serialize_column]
- [CSV.serialize_column ][onetl.file.format.csv.CSV.serialize_column]

## Примеры

Преобразование `value` в строку JSON и запись в Kafka:

    ```python
        from onetl.connection import Kafka
        from onetl.db import DBWriter
        from onetl.file.format import JSON

        df = ...  # исходные данные здесь

        # сериализация структурированных данных в JSON
        json = JSON()
        write_df = df.select(
            df.key,
            json.serialize_column(df.value),
        )

        # запись данных в Kafka
        kafka = Kafka(...)

        writer = DBWriter(
            connection=kafka,
            target="topic_name",
        )
        writer.run(write_df)
    ```

## Параметры

::: onetl.connection.db_connection.kafka.options.KafkaWriteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
        extensions:
            - griffe_sphinx
            - griffe_pydantic: {schema: false}
