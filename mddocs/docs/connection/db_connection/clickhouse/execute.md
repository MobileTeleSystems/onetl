# Executing statements in Clickhouse { #DBR-onetl-connection-db-connection-clickhouse-execute-executing-statements-in-clickhouse }

!!! warning

    Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

    Do **NOT** use them to read large amounts of data. Use [DBReader][DBR-onetl-connection-db-connection-clickhouse-read-reading-from-clickhouse-using-dbreader] or [Clickhouse.sql][DBR-onetl-connection-db-connection-clickhouse-sql-reading-from-clickhouse-using-clickhouse-sql] instead.

## How to { #DBR-onetl-connection-db-connection-clickhouse-execute-how-to }

There are 2 ways to execute some statement in Clickhouse

### Use `Clickhouse.fetch` { #DBR-onetl-connection-db-connection-clickhouse-execute-use-clickhouse-fetch }

Use this method to perform some `SELECT` query which returns **small number or rows**, like reading
Clickhouse config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts [Clickhouse.FetchOptions][onetl.connection.db_connection.clickhouse.options.ClickhouseFetchOptions].


!!! warning

    Please take into account [Clickhouse types][DBR-onetl-connection-db-connection-clickhouse-types-clickhouse-spark-type-mapping].

#### Syntax support in `Clickhouse.fetch` { #DBR-onetl-connection-db-connection-clickhouse-execute-syntax-support-in-clickhouse-fetch }

This method supports **any** query syntax supported by Clickhouse, like:

- ✅︎ `SELECT ... FROM ...`
- ✅︎ `WITH alias AS (...) SELECT ...`
- ✅︎ `SELECT func(arg1, arg2)` - call function
- ✅︎ `SHOW ...`
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples for `Clickhouse.fetch` { #DBR-onetl-connection-db-connection-clickhouse-execute-examples-for-clickhouse-fetch }

```python
from onetl.connection import Clickhouse

clickhouse = Clickhouse(...)

df = clickhouse.fetch(
    "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
    options=Clickhouse.FetchOptions(queryTimeout=10),
)
clickhouse.close()
value = df.collect()[0][0]  # get value from first row and first column
```

### Use `Clickhouse.execute` { #DBR-onetl-connection-db-connection-clickhouse-execute-use-clickhouse-execute }

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts [Clickhouse.ExecuteOptions][onetl.connection.db_connection.clickhouse.options.ClickhouseExecuteOptions].


#### Syntax support in `Clickhouse.execute` { #DBR-onetl-connection-db-connection-clickhouse-execute-syntax-support-in-clickhouse-execute }

This method supports **any** query syntax supported by Clickhouse, like:

- ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`, and so on
- ✅︎ `ALTER ...`
- ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...`, and so on
- ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE`, and so on
- ✅︎ other statements not mentioned here
- ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples for `Clickhouse.execute` { #DBR-onetl-connection-db-connection-clickhouse-execute-examples-for-clickhouse-execute }

```python
from onetl.connection import Clickhouse

clickhouse = Clickhouse(...)

clickhouse.execute("DROP TABLE schema.table")
clickhouse.execute(
    """
    CREATE TABLE schema.table (
        id UInt8,
        key String,
        value Float32
    )
    ENGINE = MergeTree()
    ORDER BY id
    """,
    options=Clickhouse.ExecuteOptions(queryTimeout=10),
)
```

## Notes { #DBR-onetl-connection-db-connection-clickhouse-execute-notes }

These methods **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

So it should **NOT** be used to read large amounts of data. Use [DBReader][DBR-onetl-connection-db-connection-clickhouse-read-reading-from-clickhouse-using-dbreader] or [Clickhouse.sql][DBR-onetl-connection-db-connection-clickhouse-sql-reading-from-clickhouse-using-clickhouse-sql] instead.

## Options { #DBR-onetl-connection-db-connection-clickhouse-execute-options }


::: onetl.connection.db_connection.clickhouse.options.ClickhouseFetchOptions
    options:
        inherited_members: true

::: onetl.connection.db_connection.clickhouse.options.ClickhouseExecuteOptions
    options:
        inherited_members: true
