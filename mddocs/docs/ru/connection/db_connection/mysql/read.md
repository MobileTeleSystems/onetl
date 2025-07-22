# Чтение из MySQL с использованием `DBReader` { #mysql-read }

[DBReader][db-reader] поддерживает [стратегии][strategy] для инкрементального чтения данных, но не поддерживает нестандартные запросы, такие как `JOIN`.

!!! warning

    Обратите внимание на [типы данных MySQL][mysql-types]

## Поддерживаемые функции DBReader

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, поддерживаемые стратегии:
  - ✅︎ [Snapshot][snapshot-strategy-0]
  - ✅︎ [Incremental][incremental-strategy-0]
  - ✅︎ [Snapshot batch][snapshot-batch-strategy-0]
  - ✅︎ [Incremental batch][incremental-batch-strategy-0]
- ✅︎ `hint` (см. [официальную документацию](https://dev.mysql.com/doc/refman/en/optimizer-hints.html))
- ❌ `df_schema`
- ✅︎ `options` (см. [MySQL.ReadOptions][onetl.connection.db_connection.mysql.options.MySQLReadOptions])

## Примеры

Стратегия Snapshot:

    ```python
        from onetl.connection import MySQL
        from onetl.db import DBReader

        mysql = MySQL(...)

        reader = DBReader(
            connection=mysql,
            source="schema.table",
            columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
            where="key = 'something'",
            hint="SKIP_SCAN(schema.table key_index)",
            options=MySQL.ReadOptions(partitionColumn="id", numPartitions=10),
        )
        df = reader.run()   
    ```

Стратегия Incremental:

    ```python
        from onetl.connection import MySQL
        from onetl.db import DBReader
        from onetl.strategy import IncrementalStrategy

        mysql = MySQL(...)

        reader = DBReader(
            connection=mysql,
            source="schema.table",
            columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
            where="key = 'something'",
            hint="SKIP_SCAN(schema.table key_index)",
            hwm=DBReader.AutoDetectHWM(name="mysql_hwm", expression="updated_dt"),
            options=MySQL.ReadOptions(partitionColumn="id", numPartitions=10),
        )

        with IncrementalStrategy():
            df = reader.run()  
    ```

## Рекомендации

### Выбирайте только необходимые столбцы

Вместо передачи `"*"` в `DBReader(columns=[...])` предпочтительно передавать точные имена столбцов. Это уменьшает объем данных, передаваемых из Oracle в Spark.

### Обратите внимание на значение `where`

Вместо фильтрации данных на стороне Spark с использованием `df.filter(df.column == 'value')`, передайте соответствующее выражение `DBReader(where="column = 'value'")`.
Это не только уменьшает объем передаваемых данных из Oracle в Spark, но и может улучшить производительность запроса.
Особенно если для столбцов, используемых в условии `where`, существуют индексы.

## Опции

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.mysql.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: MySQLReadOptions
        :inherited-members: GenericOptions
        :member-order: bysource
    ```
 -->

::: onetl.connection.db_connection.mysql.options.MySQLReadOptions
    options:
        members: true
        heading_level: 3
        show_root_heading: true
