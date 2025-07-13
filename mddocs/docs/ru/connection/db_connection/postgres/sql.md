# Чтение из Postgres с использованием `Postgres.sql` { #postgres-sql }

`Postgres.sql` позволяет передавать пользовательский SQL-запрос, но не поддерживает инкрементальные стратегии.

!!! warning

    Пожалуйста, учитывайте [типы данных Postgres][postgres-types]

!!! warning

    Запрос выполняется в соединении с режимом **чтение-запись**, поэтому если вы вызываете функции/процедуры с DDL/DML-операторами внутри, они могут изменить данные в вашей базе данных.

## Поддержка синтаксиса

Поддерживаются только запросы со следующим синтаксисом:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ❌ `SET ...; SELECT ...;` - несколько операторов не поддерживаются

## Примеры

    ```python
        from onetl.connection import Postgres

        postgres = Postgres(...)
        df = postgres.sql(
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
            options=Postgres.SQLOptions(
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
Это уменьшает объем данных, передаваемых из Postgres в Spark.

### Обращайте внимание на значения в условии `where`

Вместо фильтрации данных на стороне Spark с помощью `df.filter(df.column == 'value')` передавайте соответствующее условие `WHERE column = 'value'`.
Это не только уменьшает объем данных, передаваемых из Postgres в Spark, но также может улучшить производительность запроса.
Особенно если для столбцов, используемых в условии `where`, есть индексы или секционирование.

## Опции { #postgres-sql-options }

::: onetl.connection.db_connection.postgres.options.PostgresSQLOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
