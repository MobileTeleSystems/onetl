# Executing statements in Postgres { #DBR-onetl-connection-db-connection-postgres-execute-executing-statements-in-postgres }

!!! warning

    Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

    Do **NOT** use them to read large amounts of data. Use [DBReader][DBR-onetl-connection-db-connection-postgres-read-reading-from-postgres-using-dbreader] or [Postgres.sql][DBR-onetl-connection-db-connection-postgres-sql-reading-from-postgres-using-postgres-sql] instead.

## How to { #DBR-onetl-connection-db-connection-postgres-execute-how-to }

There are 2 ways to execute some statement in Postgres

### Use `Postgres.fetch` { #DBR-onetl-connection-db-connection-postgres-execute-use-postgres-fetch }

Use this method to execute some `SELECT` query which returns **small number or rows**, like reading Postgres config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts [Postgres.FetchOptions][onetl.connection.db_connection.postgres.options.PostgresFetchOptions].


!!! warning

    Please take into account [Postgres types][DBR-onetl-connection-db-connection-postgres-types-postgres-spark-type-mapping].

#### Syntax support in `Postgres.fetch` { #DBR-onetl-connection-db-connection-postgres-execute-syntax-support-in-postgres-fetch }

This method supports **any** query syntax supported by Postgres, like:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples for `Postgres.fetch` { #DBR-onetl-connection-db-connection-postgres-execute-examples-for-postgres-fetch }

```python
from onetl.connection import Postgres

postgres = Postgres(...)

df = postgres.fetch(
    "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
    options=Postgres.FetchOptions(queryTimeout=10),
)
postgres.close()
value = df.collect()[0][0]  # get value from first row and first column
```

### Use `Postgres.execute` { #DBR-onetl-connection-db-connection-postgres-execute-use-postgres-execute }

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts [Postgres.ExecuteOptions][onetl.connection.db_connection.postgres.options.PostgresExecuteOptions].


#### Syntax support in `Postgres.execute` { #DBR-onetl-connection-db-connection-postgres-execute-syntax-support-in-postgres-execute }

This method supports **any** query syntax supported by Postgres, like:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`, and so on
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...`, and so on
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE`, and so on
- ✅︎ `CALL procedure(arg1, arg2) ...`
- ✅︎ `SELECT func(arg1, arg2)` or `{call func(arg1, arg2)}` - special syntax for calling functions
- ✅︎ other statements not mentioned here
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples for `Postgres.execute` { #DBR-onetl-connection-db-connection-postgres-execute-examples-for-postgres-execute }

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

## Options { #DBR-onetl-connection-db-connection-postgres-execute-options }


::: onetl.connection.db_connection.postgres.options.PostgresFetchOptions
    options:
        inherited_members: true

::: onetl.connection.db_connection.postgres.options.PostgresExecuteOptions
    options:
        inherited_members: true
