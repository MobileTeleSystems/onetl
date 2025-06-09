<a id="mssql-execute"></a>

# Executing statements in MSSQL

#### WARNING
Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

Do **NOT** use them to read large amounts of data. Use [DBReader](read.md#mssql-read) or [MSSQL.sql](sql.md#mssql-sql) instead.

## How to

There are 2 ways to execute some statement in MSSQL

### Use `MSSQL.fetch`

Use this method to perform some `SELECT` query which returns **small number or rows**, like reading
MSSQL config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts `MSSQL.FetchOptions`.

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### WARNING
Please take into account [MSSQL <-> Spark type mapping](types.md#mssql-types).

#### Syntax support

This method supports **any** query syntax supported by MSSQL, like:

* ✅︎ `SELECT ... FROM ...`
* ✅︎ `WITH alias AS (...) SELECT ...`
* ✅︎ `SELECT func(arg1, arg2) FROM DUAL` - call function
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples

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

### Use `MSSQL.execute`

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts `MSSQL.ExecuteOptions`.

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### Syntax support

This method supports **any** query syntax supported by MSSQL, like:

* ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`
* ✅︎ `ALTER ...`
* ✅︎ `INSERT INTO ... AS SELECT ...`
* ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE`, and so on
* ✅︎ `EXEC procedure(arg1, arg2) ...` or `{call procedure(arg1, arg2)}` - special syntax for calling procedure
* ✅︎ `DECLARE ... BEGIN ... END` - execute PL/SQL statement
* ✅︎ other statements not mentioned here
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples

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

## Options
