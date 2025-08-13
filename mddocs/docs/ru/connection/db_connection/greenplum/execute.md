# Выполнение предложений в Greenplum { #greenplum-execute }

!!! warning

    Методы, описанные ниже, **загружают все строки** из БД **в память драйвера Spark**, а затем конвертируют их в DataFrame.

    **НЕ** используйте их для чтения больших объёмов данных. Вместо этого используйте [DBReader][greenplum-read].

## Как использовать

Существует 2 способа выполнения запросов в Greenplum

### Использование `Greenplum.fetch`

Используйте этот метод для выполнения запросов `SELECT`, которые возвращают **небольшое количество строк**, например, для чтения конфигурации Greenplum или данных из справочных таблиц. Метод возвращает Spark DataFrame.

Метод принимает [Greenplum.FetchOptions][onetl.connection.db_connection.greenplum.options.GreenplumFetchOptions].

Соединение, открытое с помощью этого метода, следует закрыть с помощью `connection.close()` или конструкции `with connection:`.

!!! warning

    `Greenplum.fetch` реализован с использованием JDBC-подключения к Postgres, поэтому типы данных обрабатываются немного иначе, чем в `DBReader`. См. [Типы данных Postgres][postgres-types].

#### Поддержка синтаксиса в `Greenplum.fetch`

Этот метод позволяет использовать **любой** синтаксис запросов, который поддерживается Greenplum, например:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ✅︎ `SELECT func(arg1, arg2)` или `{call func(arg1, arg2)}` - специальный синтаксис для вызова функций
- ❌ `SET ...; SELECT ...;` - множественные запросы не поддерживаются

#### Примеры с `Greenplum.fetch`

    ```python
    from onetl.connection import Greenplum

    greenplum = Greenplum(...)

    df = greenplum.fetch(
        "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
        options=Greenplum.FetchOptions(queryTimeout=10),
    )
    greenplum.close()
    value = df.collect()[0][0]  # получить значение из первой строки и первого столбца 
    ```

### Использование `Greenplum.execute`

Используйте этот метод для выполнения операций DDL и DML. Каждый вызов метода выполняет операцию в отдельной транзакции и затем фиксирует её.

Метод принимает [Greenplum.ExecuteOptions][onetl.connection.db_connection.greenplum.options.GreenplumExecuteOptions].

Соединение, открытое с помощью этого метода, следует закрыть с помощью `connection.close()` или конструкции `with connection:`.

#### Поддержка синтаксиса в `Greenplum.execute`

Этот метод поддерживает **любой** синтаксис запросов, который можно использовать в Greenplum, например:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...` и т.д.
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...` и т.д.
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE` и т.д.
- ✅︎ `CALL procedure(arg1, arg2) ...`
- ✅︎ `SELECT func(arg1, arg2)` или `{call func(arg1, arg2)}` - специальный синтаксис для вызова функций
- ✅︎ другие запросы, не упомянутые здесь
- ❌ `SET ...; SELECT ...;` - множественные запросы не поддерживаются

#### Примеры с `Greenplum.execute`

    ```python
    from onetl.connection import Greenplum

    greenplum = Greenplum(...)

    greenplum.execute("DROP TABLE schema.table")
    greenplum.execute(
        """
        CREATE TABLE schema.table (
            id int,
            key text,
            value real
        )
        DISTRIBUTED BY id
        """,
        options=Greenplum.ExecuteOptions(queryTimeout=10),
    ) 
    ```

## Схема взаимодействия

В отличие от чтения и записи, выполнение запросов в Greenplum происходит **только** через мастер-узел Greenplum без какого-либо взаимодействия между сегментами Greenplum и исполнителями Spark. Более того, исполнители Spark в этом случае не используются.

Единственный порт, используемый при взаимодействии с Greenplum в этом случае — `5432` (порт мастер-узла Greenplum).

??? note "Взаимодействие Spark <-> Greenplum при Greenplum.execute()/Greenplum.fetch()"

    ```plantuml

        @startuml
        title Greenplum master <-> Spark driver
                box Spark
                participant "Spark driver"
                end box

                box "Greenplum"
                participant "Greenplum master"
                end box

                == Greenplum.check() ==

                activate "Spark driver"
                "Spark driver" -> "Greenplum master" ++ : CONNECT

                == Greenplum.execute(statement) ==
                "Spark driver" --> "Greenplum master" : EXECUTE statement
                "Greenplum master" -> "Spark driver" : RETURN result

                == Greenplum.close() ==
                "Spark driver" --> "Greenplum master" : CLOSE CONNECTION

                deactivate "Greenplum master"
                deactivate "Spark driver"
        @enduml
    ```

    ```mermaid
        ---
        title: Greenplum master <—> Spark driver
        ---

        sequenceDiagram
            box Spark
            participant A as Spark driver
            end
            box Greenplum
            participant B as Greenplum master
            end

            Note over A,B: == Greenplum.check() ==

            A->>B: CONNECT

            Note over A,B: == Greenplum.execute(statement) ==

            A-->>B: EXECUTE statement
            B-->> A: RETURN result

            Note over A,B: == Greenplum.close() ==

            A ->> B: CLOSE CONNECTION
    ```

## Опции

<!-- 
    ```{eval-rst}
    .. currentmodule:: onetl.connection.db_connection.greenplum.options
    ```

    ```{eval-rst}
    .. autopydantic_model:: GreenplumFetchOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```

    ```{eval-rst}
    .. autopydantic_model:: GreenplumExecuteOptions
        :inherited-members: GenericOptions
        :member-order: bysource
        :model-show-field-summary: false
        :field-show-constraints: false
    ```
 -->

::: onetl.connection.db_connection.greenplum.options.GreenplumFetchOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true

::: onetl.connection.db_connection.greenplum.options.GreenplumExecuteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
