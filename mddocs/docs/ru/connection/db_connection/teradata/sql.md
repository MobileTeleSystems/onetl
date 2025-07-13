# Чтение из Teradata с использованием `Teradata.sql` { #teradata-sql }

`Teradata.sql` позволяет передавать пользовательский SQL запрос, но не поддерживает инкрементальные стратегии.

!!! warning

    Запрос выполняется в соединении с правами **чтения-записи**, поэтому если вы вызываете функции/процедуры, содержащие DDL/DML инструкции, они могут изменить данные в вашей базе данных.

## Поддержка синтаксиса

Поддерживаются только запросы со следующим синтаксисом:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ❌ `SHOW ...`
- ❌ `SET ...; SELECT ...;` - множественные инструкции не поддерживаются

## Примеры

    ```python
        from onetl.connection import Teradata

        teradata = Teradata(...)
        df = teradata.sql(
            """
            SELECT
                id,
                key,
                CAST(value AS VARCHAR) AS value,
                updated_at,
                HASHAMP(HASHBUCKET(HASHROW(id))) MOD 10 AS part_column
            FROM
                database.mytable
            WHERE
                key = 'something'
            """,
            options=Teradata.SQLOptions(
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
Это уменьшает объем данных, передаваемых из Teradata в Spark.

### Обращайте внимание на значения в условии `where`

Вместо фильтрации данных на стороне Spark с помощью `df.filter(df.column == 'value')` передавайте правильное условие `WHERE column = 'value'`.
Это не только уменьшает объем данных, передаваемых из Teradata в Spark, но и может повысить производительность запроса.
Особенно если существуют индексы или разделы для столбцов, используемых в условии `where`.

## Опции

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.teradata.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: TeradataSQLOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```
 -->

::: onetl.connection.db_connection.teradata.options.TeradataSQLOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
