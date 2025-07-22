# Чтение из Hive с использованием `DBReader` { #hive-read }

[DBReader][db-reader] поддерживает [стратегии][strategy] для инкрементального чтения данных, но не поддерживает пользовательские запросы, такие, например, включающие `JOIN`.

## Поддерживаемые функции DBReader

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, поддерживаемые стратегии:
  - ✅︎ [Snapshot][snapshot-strategy-0]
  - ✅︎ [Incremental][incremental-strategy-0]
  - ✅︎ [Snapshot batch][snapshot-batch-strategy-0]
  - ✅︎ [Incremental batch][incremental-batch-strategy-0]
- ❌ `hint` (не поддерживается Hive)
- ❌ `df_schema`
- ❌ `options` (используются только параметры конфигурации Spark)

!!! warning "Предупреждение"

    Фактически, `columns`, `where` и `hwm.expression` должны быть написаны с использованием синтаксиса [SparkSQL](https://spark.apache.org/docs/latest/sql-ref-syntax.html#data-retrieval-statements), а не HiveQL.

## Примеры

Стратегия Snapshot:

    ```python
    from onetl.connection import Hive
    from onetl.db import DBReader

    hive = Hive(...)

    reader = DBReader(
        connection=hive,
        source="schema.table",
        columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
        where="key = 'something'",
    )
    df = reader.run() 
    ```

Cтратегия Incremental:

    ```python
        from onetl.connection import Hive
        from onetl.db import DBReader
        from onetl.strategy import IncrementalStrategy

        hive = Hive(...)

        reader = DBReader(
            connection=hive,
            source="schema.table",
            columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
            where="key = 'something'",
            hwm=DBReader.AutoDetectHWM(name="hive_hwm", expression="updated_dt"),
        )

        with IncrementalStrategy():
            df = reader.run()
    ```

## Рекомендации

### Используйте форматы записи на основе столбцов

Предпочтительны следующие форматы записи:

- [ORC](https://spark.apache.org/docs/latest/sql-data-sources-orc.html)
- [Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
- [Iceberg](https://iceberg.apache.org/spark-quickstart/)
- [Hudi](https://hudi.apache.org/docs/quick-start-guide/)
- [Delta](https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake)

Для форматов записи на основе столбцов каждый файл содержит отдельные секции, где хранятся данные столбцов. Футер файла содержит информацию о расположении каждой секции/группы столбцов. Spark может использовать эту информацию для загрузки только тех секций, которые требуются для конкретного запроса, например, только выбранных столбцов, что значительно ускоряет выполнение запроса.

Еще одно преимущество — высокий коэффициент сжатия, например, в 10-100 раз по сравнению с JSON или CSV.

### Выбирайте только необходимые столбцы

Вместо передачи `"*"` в `DBReader(columns=[...])` предпочтительнее передавать точные имена столбцов.
Это существенно уменьшает объем данных, считываемых Spark, **если используются форматы файлов на основе столбцов**.

### Используйте столбцы секционирования в условии `where`

Запросы должны включать условие `WHERE` с фильтрами по столбцам секционирования Hive.
Это позволяет Spark читать только небольшой набор файлов (*отсечение секций*) вместо сканирования всей таблицы, что значительно повышает производительность.

Поддерживаемые операторы: `=`, `>`, `<` и `BETWEEN`, и только в сравнении с некоторым **статическим** значением.
