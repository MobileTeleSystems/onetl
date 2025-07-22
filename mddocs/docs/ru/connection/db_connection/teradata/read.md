# Чтение из Teradata с использованием `DBReader` { #teradata-read }

[DBReader][db-reader] поддерживает [стратегии][strategy] для инкрементального чтения данных, но не поддерживает пользовательские запросы, такие как `JOIN`.

## Поддерживаемые функции DBReader

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, поддерживаемые стратегии:
  - ✅︎ [Snapshot][snapshot-strategy-0]
  - ✅︎ [Incremental][incremental-strategy-0]
  - ✅︎ [Snapshot batch][snapshot-batch-strategy-0]
  - ✅︎ [Incremental-batch][incremental-batch-strategy-0]
- ❌ `hint` (не поддерживается Teradata)
- ❌ `df_schema`
- ✅︎ `options` (см. [Teradata.ReadOptions][onetl.connection.db_connection.teradata.options.TeradataReadOptions])

## Примеры

Стратегия Snapshot:

    ```python
        from onetl.connection import Teradata
        from onetl.db import DBReader

        teradata = Teradata(...)

        reader = DBReader(
            connection=teradata,
            source="database.table",
            columns=["id", "key", "CAST(value AS VARCHAR) value", "updated_dt"],
            where="key = 'something'",
            options=Teradata.ReadOptions(
                partitioning_mode="hash",
                partitionColumn="id",
                numPartitions=10,
            ),
        )
        df = reader.run()
    ```

Стратегия Incremental:

    ```python
        from onetl.connection import Teradata
        from onetl.db import DBReader
        from onetl.strategy import IncrementalStrategy

        teradata = Teradata(...)

        reader = DBReader(
            connection=teradata,
            source="database.table",
            columns=["id", "key", "CAST(value AS VARCHAR) value", "updated_dt"],
            where="key = 'something'",
            hwm=DBReader.AutoDetectHWM(name="teradata_hwm", expression="updated_dt"),
            options=Teradata.ReadOptions(
                partitioning_mode="hash",
                partitionColumn="id",
                numPartitions=10,
            ),
        )

        with IncrementalStrategy():
            df = reader.run()
    ```

## Рекомендации

### Выбирайте только необходимые столбцы

Вместо передачи `"*"` в `DBReader(columns=[...])` предпочтительнее указывать точные имена столбцов. Это уменьшает объем данных, передаваемых из Teradata в Spark.

### Обращайте внимание на значение `where`

Вместо фильтрации данных на стороне Spark с помощью `df.filter(df.column == 'value')`, передавайте соответствующее условие `DBReader(where="column = 'value'")`.
Это сокращает объем данных, отправляемых из Teradata в Spark, и может также улучшить производительность запроса.
Особенно если существуют индексы или разделы для столбцов, используемых в условии `where`.

### Читайте данные параллельно

`DBReader` может читать данные через несколько параллельных соединений, передавая `Teradata.ReadOptions(numPartitions=..., partitionColumn=...)`.

В приведенном выше примере Spark открывает 10 параллельных соединений, и данные равномерно распределяются между всеми этими соединениями с использованием выражения `HASHAMP(HASHBUCKET(HASHROW({partition_column}))) MOD {num_partitions}`.
Это позволяет отправлять каждому рабочему процессу Spark только часть данных, снижая потребление ресурсов.
`partition_column` здесь может быть столбцом таблицы любого типа.

Также возможно использовать `partitioning_mode="mod"` или `partitioning_mode="range"`, но в этом случае
`partition_column` должен быть целочисленным, не должен содержать `NULL`, и значения должны быть равномерно распределены.
Это также менее производительно, чем `partitioning_mode="hash"` из-за реализации `HASHAMP` в Teradata.

### НЕ используйте `TYPE=FASTEXPORT`

Teradata поддерживает несколько [различных типов соединений](https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BABFGFAF):

- `TYPE=DEFAULT` - выполняет обычные запросы `SELECT`
- `TYPE=FASTEXPORT` - использует специальный протокол FastExport для запросов выборки

Но `TYPE=FASTEXPORT` использует эксклюзивную блокировку исходной таблицы, поэтому невозможно использовать параллельное чтение данных несколькими рабочими процессами Spark.
Это приводит к отправке всех данных только одному рабочему процессу Spark, что медленно и потребляет много оперативной памяти.

Предпочтительнее использовать `partitioning_mode="hash"` из примера выше.

## Опции { #teradata-read-options }

::: onetl.connection.db_connection.teradata.options.TeradataReadOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
