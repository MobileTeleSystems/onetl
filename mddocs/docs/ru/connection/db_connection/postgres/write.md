# Запись в Postgres с использованием `DBWriter` { #postgres-write }

Для записи данных в Postgres используйте [DBWriter][db-writer].

!!! warning

    Пожалуйста, учитывайте [типы данных Postgres][postgres-types]

!!! warning

    Всегда рекомендуется создавать таблицу явно с помощью [Postgres.execute][postgres-execute] вместо того, чтобы полагаться на автоматическую генерацию DDL в Spark.

    Это связано с тем, что генератор DDL в Spark может создавать столбцы с другой точностью и типами, чем ожидается, что приводит к потере точности или другим проблемам.

## Примеры

    ```python
        from onetl.connection import Postgres
        from onetl.db import DBWriter

        postgres = Postgres(...)

        df = ...  # здесь данные

        writer = DBWriter(
            connection=postgres,
            target="schema.table",
            options=Postgres.WriteOptions(if_exists="append"),
        )

        writer.run(df)
    ```

## Опции { #postgres-write-options }

Метод выше принимает [Postgres.WriteOptions][onetl.connection.db_connection.postgres.options.PostgresWriteOptions]

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.postgres.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: PostgresWriteOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```
 -->

::: onetl.connection.db_connection.postgres.options.PostgresWriteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
