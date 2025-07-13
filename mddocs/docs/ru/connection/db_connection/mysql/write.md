# Запись в MySQL с использованием `DBWriter` { #mysql-write }

Для записи данных в MySQL используйте [DBWriter][db-writer].

!!! warning

    Пожалуйста, учитывайте [типы данных MySQL][mysql-types]

!!! warning

    Всегда рекомендуется создавать таблицу явно с помощью [MySQL.execute][mysql-execute] вместо того, чтобы полагаться на автоматическую генерацию DDL в Spark.

    Это связано с тем, что генератор DDL в Spark может создавать колонки с другой точностью и типами данных, чем ожидается, что приводит к потере точности или другим проблемам.

## Примеры

    ```python
        from onetl.connection import MySQL
        from onetl.db import DBWriter

        mysql = MySQL(...)

        df = ...  # здесь данные

        writer = DBWriter(
            connection=mysql,
            target="schema.table",
            options=MySQL.WriteOptions(
                if_exists="append",
                # ENGINE обязателен для MySQL
                createTableOptions="ENGINE = MergeTree() ORDER BY id",
            ),
        )

        writer.run(df)   
    ```

## Опции

Метод выше принимает [MySQL.WriteOptions][onetl.connection.db_connection.mysql.options.MySQLWriteOptions]

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.mysql.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: MySQLWriteOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```
 -->

::: onetl.connection.db_connection.mysql.options.MySQLWriteOptions
    options:
        members: true
        heading_level: 3
        show_root_heading: true
