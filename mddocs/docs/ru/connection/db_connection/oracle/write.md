# Запись в Oracle с помощью `DBWriter` { #oracle-write }

Для записи данных в Oracle используйте [DBWriter][db-writer].

!!! warning

    Пожалуйста, учитывайте [типы данных Oracle][oracle-types]

!!! warning

    Всегда рекомендуется создавать таблицу явно с помощью [Oracle.execute][oracle-execute] вместо того, чтобы полагаться на автоматическую генерацию DDL в Spark.

    Это связано с тем, что генератор DDL в Spark может создавать столбцы с другой точностью и типами, чем ожидается, что приводит к потере точности или другим проблемам.

## Примеры

    ```python
        from onetl.connection import Oracle
        from onetl.db import DBWriter

        oracle = Oracle(...)

        df = ...  # здесь находятся данные

        writer = DBWriter(
            connection=oracle,
            target="schema.table",
            options=Oracle.WriteOptions(if_exists="append"),
        )

        writer.run(df)
    ```

## Параметры

Вышеуказанный метод принимает [OracleWriteOptions][onetl.connection.db_connection.oracle.options.OracleWriteOptions]

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.oracle.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: OracleWriteOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```
 -->

::: onetl.connection.db_connection.oracle.options.OracleWriteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
