# Чтение из MSSQL с использованием `MSSQL.sql` { #mssql-sql }

`MSSQL.sql` позволяет передавать пользовательский SQL-запрос, но не поддерживает инкрементальные стратегии.

!!! warning

    Пожалуйста, учитывайте [типы данных MSSQL][mssql-types]

!!! warning

    Запрос выполняется в соединении с **чтением-записью**, поэтому если вы вызываете функции/процедуры с DDL/DML-операторами внутри, они могут изменить данные в вашей базе данных.

## Поддержка синтаксиса

Поддерживаются только запросы со следующим синтаксисом:

- ✅︎ `SELECT ... FROM ...`
- ❌ `WITH alias AS (...) SELECT ...`
- ❌ `SET ...; SELECT ...;` - множественные операторы не поддерживаются

## Примеры

    ```python
        from onetl.connection import MSSQL

        mssql = MSSQL(...)
        df = mssql.sql(
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
            options=MSSQL.SQLOptions(
                partitionColumn="id",
                numPartitions=10,
                lowerBound=0,
                upperBound=1000,
            ),
        )
    ```

## Рекомендации

### Выбирайте только необходимые столбцы

Вместо передачи `SELECT * FROM ...` предпочтительнее указывать точные имена столбцов `SELECT col1, col2, ...`.
Это уменьшает объем данных, передаваемых из MSSQL в Spark.

### Обращайте внимание на значение `where`

Вместо фильтрации данных на стороне Spark с помощью `df.filter(df.column == 'value')` передавайте правильное условие `WHERE column = 'value'`.
Это не только уменьшает объем данных, передаваемых из MSSQL в Spark, но и может улучшить производительность запроса.
Особенно если существуют индексы или секции для столбцов, используемых в условии `where`.

## Опции

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.mssql.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: MSSQLSQLOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```
 -->

::: onetl.connection.db_connection.mssql.options.MSSQLSQLOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
