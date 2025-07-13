# Выполнение запросов в Oracle { #oracle-execute }

!!! warning

    Методы, описанные ниже, **считывают все строки** из БД **в память драйвера Spark**, а затем преобразуют их в DataFrame.

    **НЕ** используйте их для чтения больших объемов данных. Вместо этого используйте [DBReader][oracle-read] или [Oracle.sql][oracle-sql].

## Как использовать

Существует 2 способа выполнения запросов в Oracle

### Использование `Oracle.fetch`

Используйте этот метод для выполнения запроса `SELECT`, который возвращает **небольшое количество строк**, например, для чтения конфигурации Oracle или данных из справочной таблицы. Метод возвращает Spark DataFrame.

Метод принимает параметры [Oracle.FetchOptions][onetl.connection.db_connection.oracle.options.OracleFetchOptions].

Соединение, открытое с помощью этого метода, следует затем закрыть с помощью `connection.close()` или конструкции `with connection:`.

!!! warning

    Пожалуйста, учитывайте [типы данных Oracle][oracle-types].

#### Поддержка синтаксиса

Этот метод позволяет использовать **любой** синтаксис запросов, поддерживаемый Oracle, например:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ✅︎ `SELECT func(arg1, arg2) FROM DUAL` - вызов функции
- ✅︎ `SHOW ...`
- ❌ `SET ...; SELECT ...;` - множественные операторы не поддерживаются

#### Примеры

    ```python
        from onetl.connection import Oracle

        oracle = Oracle(...)

        df = oracle.fetch(
            "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
            options=Oracle.FetchOptions(queryTimeout=10),
        )
        oracle.close()
        value = df.collect()[0][0]  # получение значения из первой строки и первого столбца
    ```

### Использование `Oracle.execute`

Используйте этот метод для выполнения операций DDL и DML. Каждый вызов метода выполняет операцию в отдельной транзакции, а затем фиксирует её.

Метод принимает параметры [Oracle.ExecuteOptions][onetl.connection.db_connection.oracle.options.OracleExecuteOptions].

Соединение, открытое с помощью этого метода, следует затем закрыть с помощью `connection.close()` или конструкции `with connection:`.

#### Поддержка синтаксиса

Этот метод поддерживает **любой** синтаксис запросов, применимый в Oracle, например:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...` и так далее
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE` и так далее
- ✅︎ `CALL procedure(arg1, arg2) ...` или `{call procedure(arg1, arg2)}` - специальный синтаксис для вызова процедуры
- ✅︎ `DECLARE ... BEGIN ... END` - выполнение PL/SQL выражения
- ✅︎ другие операторы, не упомянутые здесь
- ❌ `SET ...; SELECT ...;` - множественные операторы не поддерживаются

#### Примеры

    ```python
        from onetl.connection import Oracle

        oracle = Oracle(...)

        oracle.execute("DROP TABLE schema.table")
        oracle.execute(
            """
            CREATE TABLE schema.table (
                id bigint GENERATED ALWAYS AS IDENTITY,
                key VARCHAR2(4000),
                value NUMBER
            )
            """,
            options=Oracle.ExecuteOptions(queryTimeout=10),
        )
    ```

## Опции

::: onetl.connection.db_connection.oracle.options.OracleFetchOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true

::: onetl.connection.db_connection.oracle.options.OracleExecuteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
