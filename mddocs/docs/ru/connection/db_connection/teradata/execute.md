# Выполнение запросов в Teradata { #teradata-execute }

!!! warning

    Методы ниже **загружают все строки** возвращаемые из БД **в память драйвера Spark**, а затем конвертируют их в DataFrame.

    **НЕ** используйте их для чтения больших объемов данных. Используйте [DBReader][teradata-read] или [Teradata.sql][teradata-sql] вместо этого.

## Как использовать

Есть 2 способа выполнить запрос в Teradata

### Используйте `Teradata.fetch`

Используйте этот метод для выполнения запроса `SELECT`, который возвращает **небольшое количество строк**, например, для чтения конфигурации Teradata или данных из справочной таблицы. Метод возвращает Spark DataFrame.

Метод принимает [Teradata.FetchOptions][onetl.connection.db_connection.teradata.options.TeradataFetchOptions].

Соединение, открытое с помощью этого метода, должно быть затем закрыто с помощью `connection.close()` или `with connection:`.

#### Поддержка синтаксиса в `Teradata.fetch`

Этот метод поддерживает **любой** синтаксис запросов, поддерживаемый Teradata, например:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ✅︎ `SHOW ...`
- ❌ `SET ...; SELECT ...;` - множественные запросы не поддерживаются

#### Примеры с `Teradata.fetch`

    ```python
        from onetl.connection import Teradata

        teradata = Teradata(...)

        df = teradata.fetch(
            "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
            options=Teradata.FetchOptions(queryTimeout=10),
        )
        teradata.close()
        value = df.collect()[0][0]  # получить значение из первой строки и первого столбца
    ```

### Используйте `Teradata.execute`

Используйте этот метод для выполнения операций DDL и DML. Каждый вызов метода выполняет операцию в отдельной транзакции, а затем фиксирует ее.

Метод принимает [Teradata.ExecuteOptions][onetl.connection.db_connection.teradata.options.TeradataExecuteOptions].

Соединение, открытое с помощью этого метода, должно быть затем закрыто с помощью `connection.close()` или `with connection:`.

#### Поддержка синтаксиса в `Teradata.execute`

Этот метод поддерживает **любой** синтаксис запросов, поддерживаемый Teradata, например:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`, и так далее
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...`, и так далее
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE`, и так далее
- ✅︎ `CALL procedure(arg1, arg2) ...` или `{call procedure(arg1, arg2)}` - специальный синтаксис для вызова процедуры
- ✅︎ `EXECUTE macro(arg1, arg2)`
- ✅︎ `EXECUTE FUNCTION ...`
- ✅︎ другие запросы, не упомянутые здесь
- ❌ `SET ...; SELECT ...;` - множественные запросы не поддерживаются

#### Примеры с `Teradata.execute`

    ```python
        from onetl.connection import Teradata

        teradata = Teradata(...)

        teradata.execute("DROP TABLE database.table")
        teradata.execute(
            """
            CREATE MULTISET TABLE database.table AS (
                id BIGINT,
                key VARCHAR,
                value REAL
            )
            NO PRIMARY INDEX
            """,
            options=Teradata.ExecuteOptions(queryTimeout=10),
        )
    ```

## Опции { #teradata-fetch-options }

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.teradata.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: TeradataFetchOptions
        :inherited-members: GenericOptions
        :member-order: bysource

    ```

    ```{eval-rst}
    .. autopydantic_model:: TeradataExecuteOptions
        :inherited-members: GenericOptions
        :member-order: bysource
    ```
 -->

::: onetl.connection.db_connection.teradata.options.TeradataFetchOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true

::: onetl.connection.db_connection.teradata.options.TeradataExecuteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
