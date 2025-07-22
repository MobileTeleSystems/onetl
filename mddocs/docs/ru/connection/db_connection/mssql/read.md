# Чтение из MSSQL с использованием `DBReader` { #mssql-read }

[DBReader][db-reader] поддерживает [стратегии][strategy] для инкрементального чтения данных, но не поддерживает пользовательские запросы, такие как `JOIN`.

!!! warning

    Пожалуйста, учитывайте [типы данных MSSQL][mssql-types]

## Поддерживаемые функции DBReader

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, поддерживаемые стратегии:
  - ✅︎ [Snapshot][snapshot-strategy-0]
  - ✅︎ [Incremental][incremental-strategy-0]
  - ✅︎ [Snapshot batch][snapshot-batch-strategy-0]
  - ✅︎ [Incremental batch][incremental-batch-strategy-0]
- ❌ `hint` (MSSQL поддерживает подсказки, но DBReader не поддерживает, по крайней мере, пока)
- ❌ `df_schema`
- ✅︎ `options` (смотрите [MSSQL.ReadOptions][onetl.connection.db_connection.mssql.options.MSSQLReadOptions])

## Примеры

Стратегия Snapshot:

    ```python
        from onetl.connection import MSSQL
        from onetl.db import DBReader

        mssql = MSSQL(...)

        reader = DBReader(
            connection=mssql,
            source="schema.table",
            columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
            where="key = 'something'",
            options=MSSQL.ReadOptions(partitionColumn="id", numPartitions=10),
        )
        df = reader.run()
    ```

Cтратегия Incremental:

    ```python
        from onetl.connection import MSSQL
        from onetl.db import DBReader
        from onetl.strategy import IncrementalStrategy

        mssql = MSSQL(...)

        reader = DBReader(
            connection=mssql,
            source="schema.table",
            columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
            where="key = 'something'",
            hwm=DBReader.AutoDetectHWM(name="mssql_hwm", expression="updated_dt"),
            options=MSSQL.ReadOptions(partitionColumn="id", numPartitions=10),
        )

        with IncrementalStrategy():
            df = reader.run()
    ```

## Рекомендации

### Выбирайте только необходимые столбцы

Вместо передачи `"*"` в `DBReader(columns=[...])` предпочтительнее передавать точные имена столбцов. Это уменьшает объем данных, передаваемых из MSSQL в Spark.

### Обратите внимание на значение `where`

Вместо фильтрации данных на стороне Spark с помощью `df.filter(df.column == 'value')` передавайте соответствующее условие `DBReader(where="column = 'value'")`.
Это не только уменьшает количество данных, отправляемых из MSSQL в Spark, но и может улучшить производительность запроса.
Особенно если существуют индексы или разделы для столбцов, используемых в условии `where`.

## Опции

::: onetl.connection.db_connection.mssql.options.MSSQLReadOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
