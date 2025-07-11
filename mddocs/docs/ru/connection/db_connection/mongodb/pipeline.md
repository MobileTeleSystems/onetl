# Чтение из MongoDB с использованием `MongoDB.pipeline` { #mongodb-pipeline }

[MongoDB.sql][onetl.connection.db_connection.mongodb.connection.MongoDB.pipeline] позволяет передавать пользовательский пайплайн, но не поддерживает инкрементальные стратегии.

!!! warning "Предупреждение"

    Пожалуйста, учитывайте [типы данных MongoDB][mongodb-types]

## Рекомендации

### Обратите внимание на значение `pipeline`

Вместо фильтрации данных на стороне Spark с помощью `df.filter(df.column == 'value')` передавайте правильное значение `mongodb.pipeline(..., pipeline={"$match": {"column": {"$eq": "value"}}})`.
Это одновременно уменьшает объем данных, передаваемых из MongoDB в Spark, и может также улучшить производительность запроса.
Особенно если есть индексы для столбцов, используемых в значении `pipeline`.

## Справочная информация

::: onetl.connection.db_connection.mongodb.connection.MongoDB.pipeline
    options:
        heading_level: 3
        show_root_heading: true

::: onetl.connection.db_connection.mongodb.options.MongoDBPipelineOptions
    options:
        heading_level: 3
        show_root_heading: true
