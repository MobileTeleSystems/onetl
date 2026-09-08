# Executing statements in Greenplum { #DBR-onetl-connection-db-connection-greenplum-execute-executing-statements-in-greenplum }

!!! warning

    Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

    Do **NOT** use them to read large amounts of data. Use [DBReader][DBR-onetl-connection-db-connection-greenplum-read-reading-from-greenplum-using-dbreader] instead.

## How to { #DBR-onetl-connection-db-connection-greenplum-execute-how-to }

There are 2 ways to execute some statement in Greenplum

### Use `Greenplum.fetch` { #DBR-onetl-connection-db-connection-greenplum-execute-use-greenplum-fetch }

Use this method to perform some `SELECT` query which returns **small number or rows**, like reading
Greenplum config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts [Greenplum.FetchOptions][onetl.connection.db_connection.greenplum.options.GreenplumFetchOptions].


!!! warning

    `Greenplum.fetch` is implemented using Postgres JDBC connection, so types are handled a bit differently than in `DBReader`. See [Postgres types][DBR-onetl-connection-db-connection-postgres-types-postgres-spark-type-mapping].

#### Syntax support in `Greenplum.fetch` { #DBR-onetl-connection-db-connection-greenplum-execute-syntax-support-in-greenplum-fetch }

This method supports **any** query syntax supported by Greenplum, like:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ✅︎ `SELECT func(arg1, arg2)` or `{call func(arg1, arg2)}` - special syntax for calling functions
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples for `Greenplum.fetch` { #DBR-onetl-connection-db-connection-greenplum-execute-examples-for-greenplum-fetch }

```python
from onetl.connection import Greenplum

greenplum = Greenplum(...)

df = greenplum.fetch(
    "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
    options=Greenplum.FetchOptions(queryTimeout=10),
)
value = df.collect()[0][0]  # get value from first row and first column

```

### Use `Greenplum.execute` { #DBR-onetl-connection-db-connection-greenplum-execute-use-greenplum-execute }

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts [Greenplum.ExecuteOptions][onetl.connection.db_connection.greenplum.options.GreenplumExecuteOptions].


#### Syntax support in `Greenplum.execute` { #DBR-onetl-connection-db-connection-greenplum-execute-syntax-support-in-greenplum-execute }

This method supports **any** query syntax supported by Greenplum, like:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`, and so on
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...`, and so on
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE`, and so on
- ✅︎ `CALL procedure(arg1, arg2) ...`
- ✅︎ `SELECT func(arg1, arg2)` or `{call func(arg1, arg2)}` - special syntax for calling functions
- ✅︎ other statements not mentioned here
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples for `Greenplum.execute` { #DBR-onetl-connection-db-connection-greenplum-execute-examples-for-greenplum-execute }

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

## Interaction schema { #DBR-onetl-connection-db-connection-greenplum-execute-interaction-schema }

Unlike reading & writing, executing statements in Greenplum is done **only** through Greenplum master node,
without any interaction between Greenplum segments and Spark executors. More than that, Spark executors are not used in this case.

The only port used while interacting with Greenplum in this case is `5432` (Greenplum master port).

??? note "Spark <-> Greenplum interaction during Greenplum.execute()/Greenplum.fetch()"

    ```mermaid
    ---
    title: Greenplum master ↔ Spark driver
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

## Options { #DBR-onetl-connection-db-connection-greenplum-execute-options }


::: onetl.connection.db_connection.greenplum.options.GreenplumFetchOptions
    options:
        inherited_members: true

::: onetl.connection.db_connection.greenplum.options.GreenplumExecuteOptions
    options:
        inherited_members: true
