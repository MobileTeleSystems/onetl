# Чтение из Oracle с использованием `DBReader` { #oracle-read }

[DBReader][db-reader] поддерживает [стратегии][strategy] для инкрементального чтения данных, но не поддерживает пользовательские запросы, такие как `JOIN`.

!!! warning

    Пожалуйста, учитывайте особенности [типов данных Oracle][oracle-types]

## Поддерживаемые функции DBReader

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, поддерживаемые стратегии:
  - ✅︎ [Snapshot][snapshot-strategy]
  - ✅︎ [Incremental][incremental-strategy]
  - ✅︎ [Snapshot batch][snapshot-batch-strategy]
  - ✅︎ [Incremental batch][incremental-batch-strategy]
- ✅︎ `hint` (см. [официальную документацию](https://docs.oracle.com/cd/B10500_01/server.920/a96533/hintsref.htm))
- ❌ `df_schema`
- ✅︎ `options` (см. [Oracle.ReadOptions][onetl.connection.db_connection.oracle.options.OracleReadOptions])

## Примеры

Стратегия Snapshot:

    ```python
        from onetl.connection import Oracle
        from onetl.db import DBReader

        oracle = Oracle(...)

        reader = DBReader(
            connection=oracle,
            source="schema.table",
            columns=["id", "key", "CAST(value AS VARCHAR2(4000)) value", "updated_dt"],
            where="key = 'something'",
            hint="INDEX(schema.table key_index)",
            options=Oracle.ReadOptions(partitionColumn="id", numPartitions=10),
        )
        df = reader.run()
    ```

Стратегия Incremental:

    ```python
        from onetl.connection import Oracle
        from onetl.db import DBReader
        from onetl.strategy import IncrementalStrategy

        oracle = Oracle(...)

        reader = DBReader(
            connection=oracle,
            source="schema.table",
            columns=["id", "key", "CAST(value AS VARCHAR2(4000)) value", "updated_dt"],
            where="key = 'something'",
            hint="INDEX(schema.table key_index)",
            hwm=DBReader.AutoDetectHWM(name="oracle_hwm", expression="updated_dt"),
            options=Oracle.ReadOptions(partitionColumn="id", numPartitions=10),
        )

        with IncrementalStrategy():
            df = reader.run()
    ```

## Рекомендации

### Выбирайте только необходимые столбцы

Вместо использования `"*"` в `DBReader(columns=[...])` предпочтительнее указывать точные имена столбцов. Это уменьшает объем данных, передаваемых из Oracle в Spark.

### Обращайте внимание на значение `where`

Вместо фильтрации данных на стороне Spark с помощью `df.filter(df.column == 'value')` передавайте соответствующее условие `DBReader(where="column = 'value'")`.
Это как уменьшает объем данных, передаваемых из Oracle в Spark, так и может повысить производительность запроса.
Особенно, если существуют индексы или столбцы партиционирования, используемые в условии `where`.

## Опции

::: onetl.connection.db_connection.oracle.options.OracleReadOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
