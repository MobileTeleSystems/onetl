# Чтение из MongoDB с использованием `DBReader` { #mongodb-read }

[DBReader][db-reader] поддерживает [стратегии][strategy] для инкрементального чтения данных, но не поддерживает пользовательские пайплайны, например, агрегацию.

!!! warning

    Пожалуйста, учитывайте [типы данных MongoDB][mongodb-types]

## Поддерживаемые функции DBReader

- ❌ `columns` (на данный момент читаются все поля документа)
- ✅︎ `where` (передается в пайплайн агрегации `{"$match": ...}`)
- ✅︎ `hwm`, поддерживаемые стратегии:
  - ✅︎ [Snapshot][snapshot-strategy-0]
  - ✅︎ [Incremental][incremental-strategy-0]
  - ✅︎ [Snapshot batch][snapshot-batch-strategy-0]
  - ✅︎ [Incremental batch][incremental-batch-strategy-0]
  - Обратите внимание, что поле `expression` для HWM может быть только именем поля, а не пользовательским выражением
- ✅︎ `hint` (см. [официальную документацию](https://www.mongodb.com/docs/v5.0/reference/operator/meta/hint/))
- ✅︎ `df_schema` (обязательно)
- ✅︎ `options` (см. [MongoDB.ReadOptions][onetl.connection.db_connection.mongodb.options.MongoDBReadOptions])

## Примеры

Стратегия Snapshot:

    ```python
        from onetl.connection import MongoDB
        from onetl.db import DBReader

        from pyspark.sql.types import (
            StructType,
            StructField,
            IntegerType,
            StringType,
            TimestampType,
        )

        mongodb = MongoDB(...)

        # обязательно
        df_schema = StructType(
            [
                StructField("_id", StringType()),
                StructField("some", StringType()),
                StructField(
                    "field",
                    StructType(
                        [
                            StructField("nested", IntegerType()),
                        ],
                    ),
                ),
                StructField("updated_dt", TimestampType()),
            ]
        )

        reader = DBReader(
            connection=mongodb,
            source="some_collection",
            df_schema=df_schema,
            where={"field": {"$eq": 123}},
            hint={"field": 1},
            options=MongoDBReadOptions(batchSize=10000),
        )
        df = reader.run()
    ```

Стратегия Incremental:

    ```python
        from onetl.connection import MongoDB
        from onetl.db import DBReader
        from onetl.strategy import IncrementalStrategy

        from pyspark.sql.types import (
            StructType,
            StructField,
            IntegerType,
            StringType,
            TimestampType,
        )

        mongodb = MongoDB(...)

        # обязательно
        df_schema = StructType(
            [
                StructField("_id", StringType()),
                StructField("some", StringType()),
                StructField(
                    "field",
                    StructType(
                        [
                            StructField("nested", IntegerType()),
                        ],
                    ),
                ),
                StructField("updated_dt", TimestampType()),
            ]
        )

        reader = DBReader(
            connection=mongodb,
            source="some_collection",
            df_schema=df_schema,
            where={"field": {"$eq": 123}},
            hint={"field": 1},
            hwm=DBReader.AutoDetectHWM(name="mongodb_hwm", expression="updated_dt"),
            options=MongoDBReadOptions(batchSize=10000),
        )

        with IncrementalStrategy():
            df = reader.run()
    ```

## Рекомендации

### Обратите внимание на значение `where`

Вместо фильтрации данных на стороне Spark с использованием `df.filter(df.column == 'value')` передавайте соответствующее условие `DBReader(where={"column": {"$eq": "value"}})`.
Это уменьшает объем данных, передаваемых из MongoDB в Spark, и может повысить производительность запроса.
Особенно если есть индексы для столбцов, используемых в условии `where`.

## Параметры чтения

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.mongodb.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: MongoDBReadOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```
 -->

::: onetl.connection.db_connection.mongodb.options.MongoDBReadOptions
    options:
        heading_level: 3
        show_root_heading: true
