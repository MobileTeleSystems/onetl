# Чтение из Clickhouse с использованием `DBReader` { #clickhouse-read }

[DBReader][db-reader] поддерживает [стратегии][strategy] для инкрементального чтения данных, но не поддерживает пользовательские запросы, например, включающие `JOIN`.

!!! warning

    Пожалуйста, учитывайте [типы данных Сlickhouse][clickhouse-types]

## Поддерживаемые функции DBReader

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, поддерживаемые стратегии:
  - ✅︎ [Snapshot][snapshot-strategy-0]
  - ✅︎ [Incremental][incremental-strategy-0]
  - ✅︎ [Snapshot batch][snapshot-batch-strategy-0]
  - ✅︎ [Incremental batch][incremental-batch-strategy-0]
- ❌ `hint` (не поддерживается Clickhouse)
- ❌ `df_schema`
- ✅︎ `options` (см. [Clickhouse.ReadOptions][onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions])

## Примеры

### Стратегия Snapshot

    ```python
    from onetl.connection import Clickhouse
    from onetl.db import DBReader

    clickhouse = Clickhouse(...)

    reader = DBReader(
        connection=clickhouse,
        source="schema.table",
        columns=["id", "key", "CAST(value AS String) value", "updated_dt"],
        where="key = 'something'",
        options=Clickhouse.ReadOptions(partitionColumn="id", numPartitions=10),
    )
    df = reader.run()
    ```

### Стратегия Incremental

    ```python
    from onetl.connection import Clickhouse
    from onetl.db import DBReader
    from onetl.strategy import IncrementalStrategy

    clickhouse = Clickhouse(...)

    reader = DBReader(
        connection=clickhouse,
        source="schema.table",
        columns=["id", "key", "CAST(value AS String) value", "updated_dt"],
        where="key = 'something'",
        hwm=DBReader.AutoDetectHWM(name="clickhouse_hwm", expression="updated_dt"),
        options=Clickhouse.ReadOptions(partitionColumn="id", numPartitions=10),
    )

    with IncrementalStrategy():
        df = reader.run()
    ```

## Рекомендации

### Выбирайте только необходимые столбцы

Вместо передачи `"*"` в `DBReader(columns=[...])` предпочтительнее передавать точные имена столбцов. Это уменьшает объем данных, передаваемых из Clickhouse в Spark.

### Обратите внимание на значение `where`

Вместо фильтрации данных на стороне Spark с помощью `df.filter(df.column == 'value')` передавайте соответствующее условие `DBReader(where="column = 'value'")`.
Это не только уменьшает объем данных, передаваемых из Clickhouse в Spark, но и может улучшить производительность запроса.
Особенно если есть индексы или в условии `where` используются столбцы партиционирования.

## Опции { #clickhouse-read-options }

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.clickhouse.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: ClickhouseReadOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```
 -->

::: onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true