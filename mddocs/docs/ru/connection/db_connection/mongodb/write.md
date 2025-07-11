# Запись данных в MongoDB с использованием `DBWriter` { #mongodb-write }

Для записи данных в MongoDB используйте [DBWriter][db-writer].

!!! warning

    Пожалуйста, учитывайте [типы данных MongoDB][mongodb-types]

## Примеры

    ```python
        from onetl.connection import MongoDB
        from onetl.db import DBWriter

        mongodb = MongoDB(...)

        df = ...  # здесь находятся данные

        writer = DBWriter(
            connection=mongodb,
            target="schema.table",
            options=MongoDB.WriteOptions(
                if_exists="append",
            ),
        )

        writer.run(df)  
    ```

## Параметры записи

Метод выше принимает [MongoDB.WriteOptions][onetl.connection.db_connection.mongodb.options.MongoDBWriteOptions]

<!-- 

    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.mongodb.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: MongoDBWriteOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```

 -->

::: onetl.connection.db_connection.mongodb.options.MongoDBWriteOptions
    options:
        heading_level: 3
        show_root_heading: true
