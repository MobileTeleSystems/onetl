# Чтение из Oracle с использованием `Oracle.sql` { #oracle-sql }

`Oracle.sql` позволяет передавать пользовательский SQL-запрос, но не поддерживает инкрементальные стратегии.

!!! warning

    Пожалуйста, учитывайте [типы данных Oracle][oracle-types]

!!! warning

    Запрос выполняется в соединении с режимом **чтение-запись**, поэтому если вы вызываете функции/процедуры с DDL/DML-операторами внутри, они могут изменить данные в вашей базе данных.

## Поддержка синтаксиса

Поддерживаются только запросы со следующим синтаксисом:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ❌ `SHOW ...`
- ❌ `SET ...; SELECT ...;` - множественные операторы не поддерживаются

## Примеры

    ```python
        from onetl.connection import Oracle

        oracle = Oracle(...)
        df = oracle.sql(
            """
            SELECT
                id,
                key,
                CAST(value AS VARCHAR2(4000)) value,
                updated_at
            FROM
                some.mytable
            WHERE
                key = 'something'
            """,
            options=Oracle.SQLOptions(
                partitionColumn="id",
                numPartitions=10,
                lowerBound=0,
                upperBound=1000,
            ),
        )
    ```

## Рекомендации

### Выбирайте только необходимые столбцы

Вместо использования `SELECT * FROM ...` предпочтительнее указывать точные имена столбцов `SELECT col1, col2, ...`.
Это уменьшает объем данных, передаваемых из Oracle в Spark.

### Обращайте внимание на условие `where`

Вместо фильтрации данных на стороне Spark с помощью `df.filter(df.column == 'value')` передавайте соответствующее условие `WHERE column = 'value'`.
Это не только уменьшает объем данных, передаваемых из Oracle в Spark, но и может улучшить производительность запроса.
Особенно, если есть индексы или столбцы партиционирования, используемые в условии `where`.

## Опции

::: onetl.connection.db_connection.oracle.options.OracleSQLOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
