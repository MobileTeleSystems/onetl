# Запись в Clickhouse с использованием `DBWriter` { #clickhouse-write }

Для записи данных в Clickhouse используйте [DBWriter][db-writer].

!!! warning

    Пожалуйста, учитывайте [типы данных Clickhouse][clickhouse-types]

!!! warning

    Всегда рекомендуется создавать таблицу явно с помощью [Clickhouse.execute][clickhouse-execute] вместо того, чтобы полагаться на генерацию DDL таблицы в Spark.

    Это связано с тем, что генератор DDL Spark может создавать столбцы с другой точностью и типами, чем ожидается, что приводит к потере точности или другим проблемам.

## Примеры

    ```python
    from onetl.connection import Clickhouse
    from onetl.db import DBWriter

    clickhouse = Clickhouse(...)

    df = ...  # данные находятся здесь

    writer = DBWriter(
        connection=clickhouse,
        target="schema.table",
        options=Clickhouse.WriteOptions(
            if_exists="append",
            # ENGINE обязателен для Clickhouse
            createTableOptions="ENGINE = MergeTree() ORDER BY id",
        ),
    )

    writer.run(df) 
              
    ```

## Опции { #clickhouse-write-options }

Метод выше принимает [Clickhouse.WriteOptions][onetl.connection.db_connection.clickhouse.options.ClickhouseWriteOptions]

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.clickhouse.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: ClickhouseWriteOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ``` 
-->

::: onetl.connection.db_connection.clickhouse.options.ClickhouseWriteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
