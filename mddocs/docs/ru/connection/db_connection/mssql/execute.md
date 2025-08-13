# Выполнение запросов в MSSQL { #mssql-execute }

!!! warning

    Методы ниже **загружают все строки** из БД **в память драйвера Spark**, и только потом конвертируют их в DataFrame.

    **НЕ** используйте их для чтения больших объемов данных. Вместо этого используйте [DBReader][mssql-read] или [MSSQL.sql][mssql-sql].

## Как использовать

Существует 2 способа выполнения запросов в MSSQL

### Использование `MSSQL.fetch`

Используйте этот метод для выполнения запросов `SELECT`, которые возвращают **небольшое количество строк**, например, для чтения конфигурации MSSQL или данных из справочной таблицы. Метод возвращает Spark DataFrame.

Метод принимает [MSSQL.FetchOptions][onetl.connection.db_connection.mssql.options.MSSQLFetchOptions].

Соединение, открытое с использованием этого метода, следует затем закрыть с помощью `connection.close()` или `with connection:`.

!!! warning

    Пожалуйста, учитывайте [типы данных MSSQL][mssql-types].

#### Поддержка синтаксиса в `MSSQL.fetch`

Этот метод поддерживает **любой** синтаксис запросов, поддерживаемый MSSQL, например:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ✅︎ `SELECT func(arg1, arg2) FROM DUAL` - вызов функции
- ❌ `SET ...; SELECT ...;` - несколько запросов не поддерживаются

#### Примеры с `MSSQL.fetch`

    ```python
        from onetl.connection import MSSQL

        mssql = MSSQL(...)

        df = mssql.fetch(
            "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
            options=MSSQL.FetchOptions(queryTimeout=10),
        )
        mssql.close()
        value = df.collect()[0][0]  # получение значения из первой строки и первого столбца
    ```

### Использование `MSSQL.execute`

Используйте этот метод для выполнения операций DDL и DML. Каждый вызов метода выполняет операцию в отдельной транзакции и затем фиксирует ее.

Метод принимает [MSSQL.ExecuteOptions][onetl.connection.db_connection.mssql.options.MSSQLExecuteOptions].

Соединение, открытое с использованием этого метода, следует затем закрыть с помощью `connection.close()` или `with connection:`.

#### Поддержка синтаксиса в `MSSQL.execute`

Этот метод позволяет использовать **любой** синтаксис запросов, поддерживаемый MSSQL, например:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... AS SELECT ...`
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE` и т.д.
- ✅︎ `EXEC procedure(arg1, arg2) ...` или `{call procedure(arg1, arg2)}` - специальный синтаксис для вызова процедуры
- ✅︎ `DECLARE ... BEGIN ... END` - выполнение PL/SQL-выражения
- ✅︎ другие запросы, не упомянутые здесь
- ❌ `SET ...; SELECT ...;` - несколько запросов не поддерживаются

#### Примеры с `MSSQL.execute`

    ```python
        from onetl.connection import MSSQL

        mssql = MSSQL(...)

        mssql.execute("DROP TABLE schema.table")
        mssql.execute(
            """
            CREATE TABLE schema.table (
                id bigint GENERATED ALWAYS AS IDENTITY,
                key VARCHAR2(4000),
                value NUMBER
            )
            """,
            options=MSSQL.ExecuteOptions(queryTimeout=10),
        )
    ```

## Опции

::: onetl.connection.db_connection.mssql.options.MSSQLFetchOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true

::: onetl.connection.db_connection.mssql.options.MSSQLExecuteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
