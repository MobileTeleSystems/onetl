# Выполнение запросов в Clickhouse { #clickhouse-execute }

!!! warning

    Методы, описанные ниже, **загружают все строки**, возвращаемые из БД, **в память драйвера Spark**, а затем преобразуют их в DataFrame.

    **НЕ** используйте их для чтения больших объемов данных. Вместо этого используйте [DBReader][clickhouse-read] или [Clickhouse.sql][clickhouse-sql].

## Как использовать

Есть два способа выполнить запрос в Clickhouse:

### Использование `Clickhouse.fetch`

Используйте этот метод для выполнения `SELECT`-запросов, которые возвращают **небольшое количество строк**, например, чтение конфигурации Clickhouse или получение данных из справочной таблицы. Метод возвращает Spark DataFrame.

Метод принимает [Clickhouse.FetchOptions][onetl.connection.db_connection.clickhouse.options.ClickhouseFetchOptions].

Соединение, открытое с использованием этого метода, должно быть закрыто через `connection.close()` или `with connection:`.

!!! warning

    Пожалуйста, учтите [типы данных Clickhouse][clickhouse-types].

#### Поддержка синтаксиса

Этот метод позволяет использовать **любой** синтаксис запросов, поддерживаемый Clickhouse, например:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ✅︎ `SELECT func(arg1, arg2)` — вызов функции
- ✅︎ `SHOW ...`
- ❌ `SET ...; SELECT ...;` — множественные запросы не поддерживаются

#### Примеры

    ```python
    from onetl.connection import Clickhouse

    clickhouse = Clickhouse(...)

    df = clickhouse.fetch(
        "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
        options=Clickhouse.FetchOptions(queryTimeout=10),
    )
    clickhouse.close()
    value = df.collect()[0][0]  # получаем значение из первой строки и первого столбца
    ```

### Использование `Clickhouse.execute`

Используйте этот метод для выполнения операций DDL (определение данных) и DML (манипуляция данными). Каждый вызов метода выполняет операцию в отдельной транзакции, после чего она фиксируется.

Метод принимает [Clickhouse.ExecuteOptions][onetl.connection.db_connection.clickhouse.options.ClickhouseExecuteOptions].

Соединение, открытое с использованием этого метода, должно быть закрыто через `connection.close()` или `with connection:`.

#### Поддержка синтаксиса

Этот метод поддерживает **любой** синтаксис запросов, используемый Clickhouse, например:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`, и т.д.
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...`, и т.д.
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE`, и т.д.
- ✅︎ другие утверждения, не упомянутые здесь
- ❌ `SET ...; SELECT ...;` — множественные запросы не поддерживаются

#### Примеры

    ```python
    from onetl.connection import Clickhouse

    clickhouse = Clickhouse(...)

    clickhouse.execute("DROP TABLE schema.table")
    clickhouse.execute(
        """
        CREATE TABLE schema.table (
            id UInt8,
            key String,
            value Float32
        )
        ENGINE = MergeTree()
        ORDER BY id
        """,
        options=Clickhouse.ExecuteOptions(queryTimeout=10),
    )
    ```

## Примечания

Эти методы **прочитывают все строки**, возвращаемые из БД, **в память драйвера Spark**, а затем преобразуют их в DataFrame.

Поэтому их **не следует** использовать для чтения больших объемов данных. Вместо этого используйте [DBReader][clickhouse-read] или [Clickhouse.sql][clickhouse-sql].

## Опции { #clickhouse-execute-options }

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.clickhouse.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: ClickhouseFetchOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false

    ```

    ```{eval-rst}
    .. autopydantic_model:: ClickhouseExecuteOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```
 -->

::: onetl.connection.db_connection.clickhouse.options.ClickhouseFetchOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true

::: onetl.connection.db_connection.clickhouse.options.ClickhouseExecuteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
