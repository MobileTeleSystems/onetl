# Запись в MSSQL с помощью `DBWriter` { #mssql-write }

Для записи данных в MSSQL используйте [DBWriter][db-writer].

!!! warning

    Пожалуйста, учитывайте [типы данных MSSQL][mssql-types]

!!! warning

    Всегда рекомендуется создавать таблицу явно с помощью [MSSQL.execute][mssql-execute] вместо того, чтобы полагаться на автоматическую генерацию DDL Spark.

    Это связано с тем, что генератор DDL Spark может создавать столбцы с другой точностью и типами, чем ожидается, что приводит к потере точности или другим проблемам.

## Примеры

    ```python
        from onetl.connection import MSSQL
        from onetl.db import DBWriter

        mssql = MSSQL(...)

        df = ...  # данные находятся здесь

        writer = DBWriter(
            connection=mssql,
            target="schema.table",
            options=MSSQL.WriteOptions(if_exists="append"),
        )

        writer.run(df)
    ```

## Опции

Метод выше принимает [MSSQL.WriteOptions][onetl.connection.db_connection.mssql.options.MSSQLWriteOptions]

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.mssql.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: MSSQLWriteOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
```
-->

::: onetl.connection.db_connection.mssql.options.MSSQLWriteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
