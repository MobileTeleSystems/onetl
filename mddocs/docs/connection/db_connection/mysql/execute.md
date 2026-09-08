# Executing statements in MySQL { #DBR-onetl-connection-db-connection-mysql-execute-executing-statements-in-mysql }

!!! warning

    Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

    Do **NOT** use them to read large amounts of data. Use [DBReader][DBR-onetl-connection-db-connection-mysql-read-reading-from-mysql-using-dbreader] or [MySQL.sql][DBR-onetl-connection-db-connection-mysql-sql-reading-from-mysql-using-mysql-sql] instead.

## How to { #DBR-onetl-connection-db-connection-mysql-execute-how-to }

There are 2 ways to execute some statement in MySQL

### Use `MySQL.fetch` { #DBR-onetl-connection-db-connection-mysql-execute-use-mysql-fetch }

Use this method to perform some `SELECT` query which returns **small number or rows**, like reading MySQL config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts [MySQL.FetchOptions][onetl.connection.db_connection.mysql.options.MySQLFetchOptions].


!!! warning

    Please take into account [MySQL types][DBR-onetl-connection-db-connection-mysql-types-mysql-spark-type-mapping].

#### Syntax support for `MySQL.fetch` { #DBR-onetl-connection-db-connection-mysql-execute-syntax-support-for-mysql-fetch }

This method supports **any** query syntax supported by MySQL, like:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ✅︎ `SELECT func(arg1, arg2)` or `{?= call func(arg1, arg2)}` - special syntax for calling function
- ✅︎ `SHOW ...`
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples in `MySQL.fetch` { #DBR-onetl-connection-db-connection-mysql-execute-examples-in-mysql-fetch }

```python
from onetl.connection import MySQL

mysql = MySQL(...)

df = mysql.fetch(
    "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
    options=MySQL.FetchOptions(queryTimeout=10),
)
value = df.collect()[0][0]  # get value from first row and first column
```

### Use `MySQL.execute` { #DBR-onetl-connection-db-connection-mysql-execute-use-mysql-execute }

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts [MySQL.ExecuteOptions][onetl.connection.db_connection.mysql.options.MySQLExecuteOptions].


#### Syntax support in `MySQL.execute` { #DBR-onetl-connection-db-connection-mysql-execute-syntax-support-in-mysql-execute }

This method supports **any** query syntax supported by MySQL, like:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`, and so on
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...`, and so on
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, and so on
- ✅︎ `CALL procedure(arg1, arg2) ...` or `{call procedure(arg1, arg2)}` - special syntax for calling procedure
- ✅︎ other statements not mentioned here
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples for `MySQL.execute` { #DBR-onetl-connection-db-connection-mysql-execute-examples-for-mysql-execute }

```python
from onetl.connection import MySQL

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

## Options { #DBR-onetl-connection-db-connection-mysql-execute-options }


::: onetl.connection.db_connection.mysql.options.MySQLFetchOptions
    options:
        inherited_members: true
        members: true

::: onetl.connection.db_connection.mysql.options.MySQLExecuteOptions
    options:
        inherited_members: true
        members: true
