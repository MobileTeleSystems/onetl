# Выполнение операторов в Postgres { #postgres-execute }

!!! warning

    Методы ниже **считывают все строки**, возвращаемые из БД, **в память драйвера Spark**, а затем преобразуют их в DataFrame.

    **НЕ** используйте их для чтения больших объемов данных. Вместо этого используйте [DBReader][postgres-read] или [Postgres.sql][postgres-sql].

## Как использовать

Существует 2 способа выполнения операторов в Postgres

### Использование `Postgres.fetch`

Используйте этот метод для выполнения запроса `SELECT`, который возвращает **небольшое количество строк**, например, для чтения конфигурации Postgres или данных из справочной таблицы. Метод возвращает Spark DataFrame.

Метод принимает [Postgres.FetchOptions][onetl.connection.db_connection.postgres.options.PostgresFetchOptions].

Соединение, открытое с помощью этого метода, следует затем закрыть с помощью `connection.close()` или `with connection:`.

!!! warning

    Пожалуйста, учитывайте [типы данных Postgres][postgres-types].

#### Поддержка синтаксиса в `Postgres.fetch`

Этот метод поддерживает **любой** синтаксис запросов, поддерживаемый Postgres, например:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ❌ `SET ...; SELECT ...;` - множественные операторы не поддерживаются

#### Примеры с `Postgres.fetch`

    ```python
        from onetl.connection import Postgres

        postgres = Postgres(...)

        df = postgres.fetch(
            "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
            options=Postgres.FetchOptions(queryTimeout=10),
        )
        postgres.close()
        value = df.collect()[0][0]  # получить значение из первой строки и первого столбца
    ```

### Использование `Postgres.execute`

Используйте этот метод для выполнения операций DDL и DML. Каждый вызов метода выполняет операцию в отдельной транзакции, а затем фиксирует её.

Метод принимает [Postgres.ExecuteOptions][onetl.connection.db_connection.postgres.options.PostgresExecuteOptions].

Соединение, открытое с помощью этого метода, следует затем закрыть с помощью `connection.close()` или `with connection:`.

#### Поддержка синтаксиса в `Postgres.execute`

Этот метод поддерживает **любой** синтаксис запросов, поддерживаемый Postgres, например:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...` и так далее
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...` и так далее
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE` и так далее
- ✅︎ `CALL procedure(arg1, arg2) ...`
- ✅︎ `SELECT func(arg1, arg2)` или `{call func(arg1, arg2)}` - специальный синтаксис для вызова функций
- ✅︎ другие операторы, не упомянутые здесь
- ❌ `SET ...; SELECT ...;` - множественные операторы не поддерживаются

#### Примеры с `Postgres.execute`

    ```python
        from onetl.connection import Postgres

        postgres = Postgres(...)

        postgres.execute("DROP TABLE schema.table")
        postgres.execute(
            """
            CREATE TABLE schema.table (
                id bigint GENERATED ALWAYS AS IDENTITY,
                key text,
                value real
            )
            """,
            options=Postgres.ExecuteOptions(queryTimeout=10),
        )
    ```

## Параметры { #postgres-fetch-options }

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.postgres.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: PostgresFetchOptions
        :inherited-members: GenericOptions
        :member-order: bysource

    ```

    ```{eval-rst}
    .. autopydantic_model:: PostgresExecuteOptions
        :inherited-members: GenericOptions
        :member-order: bysource
    ```
 -->

::: onetl.connection.db_connection.postgres.options.PostgresFetchOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true

::: onetl.connection.db_connection.postgres.options.PostgresExecuteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
