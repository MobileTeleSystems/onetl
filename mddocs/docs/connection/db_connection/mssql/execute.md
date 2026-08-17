# Executing statements in MSSQL { #DBR-onetl-connection-db-connection-mssql-execute-executing-statements-in-mssql }

!!! warning

    Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

    Do **NOT** use them to read large amounts of data. Use [DBReader][DBR-onetl-connection-db-connection-mssql-read-reading-from-mssql-using-dbreader] or [MSSQL.sql][DBR-onetl-connection-db-connection-mssql-sql-reading-from-mssql-using-mssql-sql] instead.

## How to { #DBR-onetl-connection-db-connection-mssql-execute-how-to }

There are 2 ways to execute some statement in MSSQL

### Use `MSSQL.fetch` { #DBR-onetl-connection-db-connection-mssql-execute-use-mssql-fetch }

Use this method to perform some `SELECT` query which returns **small number or rows**, like reading MSSQL config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts [MSSQL.FetchOptions][onetl.connection.db_connection.mssql.options.MSSQLFetchOptions].


!!! warning

    Please take into account [MSSQL types][DBR-onetl-connection-db-connection-mssql-types-mssql-spark-type-mapping].

#### Syntax support in `MSSQL.fetch` { #DBR-onetl-connection-db-connection-mssql-execute-syntax-support-in-mssql-fetch }

This method supports **any** query syntax supported by MSSQL, like:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ✅︎ `SELECT func(arg1, arg2) FROM DUAL` - call function
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples for `MSSQL.fetch` { #DBR-onetl-connection-db-connection-mssql-execute-examples-for-mssql-fetch }

```python
from onetl.connection import MSSQL

mssql = MSSQL(...)

df = mssql.fetch(
    "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
    options=MSSQL.FetchOptions(queryTimeout=10),
)
mssql.close()
value = df.collect()[0][0]  # get value from first row and first column
```

### Use `MSSQL.execute` { #DBR-onetl-connection-db-connection-mssql-execute-use-mssql-execute }

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts [MSSQL.ExecuteOptions][onetl.connection.db_connection.mssql.options.MSSQLExecuteOptions].


#### Syntax support in `MSSQL.execute` { #DBR-onetl-connection-db-connection-mssql-execute-syntax-support-in-mssql-execute }

This method supports **any** query syntax supported by MSSQL, like:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... AS SELECT ...`
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE`, and so on
- ✅︎ `EXEC procedure(arg1, arg2) ...` or `{call procedure(arg1, arg2)}` - special syntax for calling procedure
- ✅︎ `DECLARE ... BEGIN ... END` - execute PL/SQL statement
- ✅︎ other statements not mentioned here
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples for `MSSQL.execute` { #DBR-onetl-connection-db-connection-mssql-execute-examples-for-mssql-execute }

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

## Options { #DBR-onetl-connection-db-connection-mssql-execute-options }


::: onetl.connection.db_connection.mssql.options.MSSQLFetchOptions
    options:
        inherited_members: true

::: onetl.connection.db_connection.mssql.options.MSSQLExecuteOptions
    options:
        inherited_members: true
