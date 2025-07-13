# Выполнение предложений в MySQL { #mysql-execute }

!!! warning

    Методы ниже **читают все строки**, возвращенные из БД **в память драйвера Spark**, и затем преобразуют их в DataFrame.

    **НЕ** используйте их для чтения **большого количества данных**. Используйте [DBReader][mysql-read] или [MySQL.sql][mysql-sql] вместо этого.

## Как это сделать

Есть 2 способа выполнить некоторый запрос в MySQL

### Использование `MySQL.fetch`

Используйте этот метод для выполнения некоторого `SELECT` запроса, который возвращает **малое количество строк**, например, чтение MySQL конфигурации или чтение данных из некоторой справочной таблицы. Метод возвращает Spark DataFrame.

Метод принимает [MySQL.FetchOptions][onetl.connection.db_connection.mysql.options.MySQLFetchOptions].

Подключение, открытый с помощью этого метода, должно быть затем закрыто с помощью `connection.close()` или `with connection:`.

!!! warning

    Пожалуйста, учитывайте [типы MySQL][mysql-types].

#### Поддержка синтаксиса

Этот метод поддерживает **любой** синтаксис запроса, поддерживаемый MySQL, например:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ✅︎ `SELECT func(arg1, arg2)` или `{?= call func(arg1, arg2)}` - специальный синтаксис для вызова функции
- ✅︎ `SHOW ...`
- ❌ `SET ...; SELECT ...;` - несколько запросов не поддерживаются

#### Примеры

    ```python
        от onetl.connection import MySQL

        mysql = MySQL(...)

        df = mysql.fetch(
            "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
            options=MySQL.FetchOptions(queryTimeout=10),
        )
        mysql.close()
        значение = df.collect()[0][0]  # получить значение из первой строки и первой колонки
    ```

### Использование `MySQL.execute`

Используйте этот метод для выполнения DDL и DML операций. Каждый вызов метода выполняет операцию в отдельной транзакции и затем коммитит ее.

Метод принимает [MySQL.ExecuteOptions][onetl.connection.db_connection.mysql.options.MySQLExecuteOptions].

Подключение, открытое с помощью этого метода, должно быть затем закрыто с помощью `connection.close()` или `with connection:`.

#### Поддержка синтаксиса

Этот метод поддерживает **любой** синтаксис запроса, поддерживаемый MySQL, например:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`, и так далее
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...`, и так далее
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, и так далее
- ✅︎ `CALL procedure(arg1, arg2) ...` или `{call procedure(arg1, arg2)}` - специальный синтаксис для вызова процедуры
- ✅︎ другие запросы, не упомянутые здесь
- ❌ `SET ...; SELECT ...;` - несколько запросов не поддерживаются

#### Примеры

    ```python
        от onetl.connection import MySQL

        mysql = MySQL(...)

        mysql.execute("DROP TABLE schema.table")
        mysql.execute(
            """
            CREATE TABLE schema.table (
                id bigint,
                key text,
                value float
            )
            ENGINE = InnoDB
            """,
            options=MySQL.ExecuteOptions(queryTimeout=10),
        )
    ```

## Настройки

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.mysql.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: MySQLFetchOptions
        :inherited-members: GenericOptions
        :member-order: bysource

    ```

    ```{eval-rst}
    .. autopydantic_model:: MySQLExecuteOptions
        :inherited-members: GenericOptions
        :member-order: bysource
    ```
 -->

::: onetl.connection.db_connection.mysql.options.MySQLFetchOptions
    options:
        members: true
        heading_level: 3
        show_root_heading: true

::: onetl.connection.db_connection.mysql.options.MySQLExecuteOptions
    options:
        members: true
        heading_level: 3
        show_root_heading: true
