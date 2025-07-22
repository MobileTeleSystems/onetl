# Чтение из Postgres с использованием `DBReader` { #postgres-read }

[DBReader][db-reader] поддерживает [стратегии][strategy] для инкрементального чтения данных, но не поддерживает пользовательские запросы, такие как `JOIN`.

!!! warning

    Пожалуйста, учитывайте [типы данных Postgres][postgres-types]

## Поддерживаемые функции DBReader

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, поддерживаемые стратегии:
  - ✅︎ [Snapshot][snapshot-strategy-0]
  - ✅︎ [Incremental][incremental-strategy-0]
  - ✅︎ [Snapshot batch][snapshot-batch-strategy-0]
  - ✅︎ [Incremental batch][incremental-batch-strategy-0]
- ❌ `hint` (не поддерживается Postgres)
- ❌ `df_schema`
- ✅︎ `options` (см. [Postgres.ReadOptions][onetl.connection.db_connection.postgres.options.PostgresReadOptions])

## Примеры

Стратегия Snapshot:

    ```python
        from onetl.connection import Postgres
        from onetl.db import DBReader

        postgres = Postgres(...)

        reader = DBReader(
            connection=postgres,
            source="schema.table",
            columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
            where="key = 'something'",
            options=Postgres.ReadOptions(partitionColumn="id", numPartitions=10),
        )
        df = reader.run()
    ```

Стратегия Incremental:

    ```python
        from onetl.connection import Postgres
        from onetl.db import DBReader
        from onetl.strategy import IncrementalStrategy

        postgres = Postgres(...)

        reader = DBReader(
            connection=postgres,
            source="schema.table",
            columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
            where="key = 'something'",
            hwm=DBReader.AutoDetectHWM(name="postgres_hwm", expression="updated_dt"),
            options=Postgres.ReadOptions(partitionColumn="id", numPartitions=10),
        )

        with IncrementalStrategy():
            df = reader.run()
    ```

## Рекомендации

### Выбирайте только требуемые столбцы

Вместо передачи `"*"` в `DBReader(columns=[...])` предпочтительнее передавать точные имена столбцов. Это уменьшает объем данных, передаваемых из Postgres в Spark.

### Обратите внимание на значение `where`

Вместо фильтрации данных на стороне Spark с помощью `df.filter(df.column == 'value')` передавайте правильное условие `DBReader(where="column = 'value'")`.
Это не только уменьшает объем данных, отправляемых из Postgres в Spark, но и может улучшить производительность запроса.
Особенно если существуют индексы или секции для столбцов, используемых в условии `where`.

## Опции { #postgres-read-options }

::: onetl.connection.db_connection.postgres.options.PostgresReadOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
