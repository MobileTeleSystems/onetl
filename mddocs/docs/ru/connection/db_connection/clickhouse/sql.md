# Чтение из Clickhouse с использованием `Clickhouse.sql` { #clickhouse-sql }

`Clickhouse.sql` позволяет передавать пользовательский SQL-запрос, но не поддерживает инкрементальные стратегии.

!!! warning

    Пожалуйста, учитывайте [типы данных Clickhouse][clickhouse-types]

!!! warning

    Запрос выполняется в соединении с режимом **чтение-запись**, поэтому если вы вызываете функции/процедуры, содержащие внутри DDL/DML-операторы, они могут изменить данные в вашей базе данных.

## Поддержка синтаксиса

Поддерживаются только запросы со следующим синтаксисом:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ❌ `SET ...; SELECT ...;` - множественные операторы не поддерживаются

## Примеры

    ```python
    from onetl.connection import Clickhouse

    clickhouse = Clickhouse(...)
    df = clickhouse.sql(
        """
        SELECT
            id,
            key,
            CAST(value AS String) value,
            updated_at
        FROM
            some.mytable
        WHERE
            key = 'something'
        """,
        options=Clickhouse.SQLOptions(
            partitionColumn="id",
            numPartitions=10,
            lowerBound=0,
            upperBound=1000,
        ),
    )
    ```

## Рекомендации

### Выбирайте только нужные столбцы

Вместо использования `SELECT * FROM ...` предпочтительнее указывать точные имена столбцов `SELECT col1, col2, ...`.
Это уменьшает объем данных, передаваемых из Clickhouse в Spark.

### Обращайте внимание на условие `where`

Вместо фильтрации данных на стороне Spark с помощью `df.filter(df.column == 'value')` передавайте правильное условие `WHERE column = 'value'`.
Это не только уменьшает объем данных, передаваемых из Clickhouse в Spark, но также может улучшить производительность запроса.
Особенно если для столбцов, используемых в условии `where`, созданы индексы или партиции.

## Опции

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.clickhouse.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: ClickhouseSQLOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```
 -->

::: onetl.connection.db_connection.clickhouse.options.ClickhouseSQLOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
