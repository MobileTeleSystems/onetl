# Чтение из MySQL с помощью `MySQL.sql` { #mysql-sql }

`MySQL.sql` позволяет передавать пользовательский SQL-запрос, но не поддерживает инкрементальные стратегии.

!!! warning

    Пожалуйста, учитывайте [MySQL типы][mysql-types]

!!! warning

    Оператор выполняется в соединении **read-write**, поэтому если вы вызываете какие-то функции/процедуры с DDL/DML операторами внутри, они могут изменить данные в вашей базе данных.

## Поддержка синтаксиса

Поддерживаются только запросы со следующим синтаксисом:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ❌ `SHOW ...`
- ❌ `SET ...; SELECT ...;` - несколько операторов не поддерживается

## Примеры

    ```python
        from onetl.connection import MySQL

        mysql = MySQL(...)
        df = mysql.sql(
            """
            SELECT
                id,
                key,
                CAST(value AS text) value,
                updated_at
            FROM
                some.mytable
            WHERE
                key = 'something'
            """,
            options=MySQL.SQLOptions(
                partitionColumn="id",
                numPartitions=10,
                lowerBound=0,
                upperBound=1000,
            ),
        )
    ```

## Рекомендации

### Выбирайте только необходимые столбцы

Вместо передачи `SELECT * FROM ...` предпочитайте указывать точные имена столбцов `SELECT col1, col2, ...`.
Это уменьшает количество данных, передаваемых из MySQL в Spark.

### Обратите внимание на значение `where`

Вместо фильтрации данных на стороне Spark с помощью `df.filter(df.column == 'value')` указывайте соответствующее условие `WHERE column = 'value'`.
Это уменьшает объем данных, отправляемых из MySQL в Spark, а также может улучшить производительность запроса.
Особенно если существуют индексы или в условии `where`, используются столбцы партиционирования.

## Опции

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.mysql.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: MySQLSQLOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```
 -->

::: onetl.connection.db_connection.mysql.options.MySQLSQLOptions
    options:
        members: true
        heading_level: 3
        show_root_heading: true
