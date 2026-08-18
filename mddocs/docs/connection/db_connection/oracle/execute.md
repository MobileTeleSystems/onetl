# Executing statements in Oracle { #DBR-onetl-connection-db-connection-oracle-execute-executing-statements-in-oracle }

!!! warning

    Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

    Do **NOT** use them to read large amounts of data. Use [DBReader][DBR-onetl-connection-db-connection-oracle-read-reading-from-oracle-using-dbreader] or [Oracle.sql][DBR-onetl-connection-db-connection-oracle-sql-reading-from-oracle-using-oracle-sql] instead.

## How to { #DBR-onetl-connection-db-connection-oracle-execute-how-to }

There are 2 ways to execute some statement in Oracle

### Use `Oracle.fetch` { #DBR-onetl-connection-db-connection-oracle-execute-use-oracle-fetch }

Use this method to execute some `SELECT` query which returns **small number or rows**, like reading
Oracle config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts [Oracle.FetchOptions][onetl.connection.db_connection.oracle.options.OracleFetchOptions].


!!! warning

    Please take into account [Oracle types][DBR-onetl-connection-db-connection-oracle-types-oracle-spark-type-mapping].

#### Syntax support in `Oracle.fetch` { #DBR-onetl-connection-db-connection-oracle-execute-syntax-support-in-oracle-fetch }

This method supports **any** query syntax supported by Oracle, like:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ✅︎ `SELECT func(arg1, arg2) FROM DUAL` - call function
- ✅︎ `SHOW ...`
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples for `Oracle.fetch` { #DBR-onetl-connection-db-connection-oracle-execute-examples-for-oracle-fetch }

```python
from onetl.connection import Oracle

oracle = Oracle(...)

df = oracle.fetch(
    "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
    options=Oracle.FetchOptions(queryTimeout=10),
)
oracle.close()
value = df.collect()[0][0]  # get value from first row and first column
```

### Use `Oracle.execute` { #DBR-onetl-connection-db-connection-oracle-execute-use-oracle-execute }

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts [Oracle.ExecuteOptions][onetl.connection.db_connection.oracle.options.OracleExecuteOptions].


#### Syntax support in `Oracle.execute` { #DBR-onetl-connection-db-connection-oracle-execute-syntax-support-in-oracle-execute }

This method supports **any** query syntax supported by Oracle, like:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...`, and so on
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE`, and so on
- ✅︎ `CALL procedure(arg1, arg2) ...` or `{call procedure(arg1, arg2)}` - special syntax for calling procedure
- ✅︎ `DECLARE ... BEGIN ... END` - execute PL/SQL statement
- ✅︎ other statements not mentioned here
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples for `Oracle.execute` { #DBR-onetl-connection-db-connection-oracle-execute-examples-for-oracle-execute }

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

## Options { #DBR-onetl-connection-db-connection-oracle-execute-options }


::: onetl.connection.db_connection.oracle.options.OracleFetchOptions
    options:
        inherited_members: true

::: onetl.connection.db_connection.oracle.options.OracleExecuteOptions
    options:
        inherited_members: true
