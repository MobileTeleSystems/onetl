# Выполнение предложений в Hive { #hive-execute }

Используйте `Hive.execute(...)` для выполнения операций DDL и DML.

## Поддержка синтаксиса

Этот метод позволяет использовать **любой** синтаксис запросов, поддерживаемый Hive, например:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...` и так далее
- ✅︎ `LOAD DATA ...` и так далее
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... SELECT ...` и так далее
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...` и так далее
- ✅︎ `MSCK REPAIR TABLE ...` и так далее
- ✅︎ другие запросы, не упомянутые здесь
- ❌ `SET ...; SELECT ...;` - множественные запросы не поддерживаются

!!! warning

    Фактически, запрос должен быть написан с использованием синтаксиса [SparkSQL](https://spark.apache.org/docs/latest/sql-ref-syntax.html#ddl-statements), а не HiveQL.

## Примеры

    ```python
        from onetl.connection import Hive

        hive = Hive(...)

        hive.execute("DROP TABLE schema.table")
        hive.execute(
            """
            CREATE TABLE schema.table (
                id NUMBER,
                key VARCHAR,
                value DOUBLE
            )
            PARTITION BY (business_date DATE)
            STORED AS orc
            """
        )
    ```

### Подробности

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.hive.connection
    ```

    ```{eval-rst}
    .. automethod:: Hive.execute
    ```
 -->

::: onetl.connection.db_connection.hive.connection.Hive.execute
    options:
        members:
            - execute
