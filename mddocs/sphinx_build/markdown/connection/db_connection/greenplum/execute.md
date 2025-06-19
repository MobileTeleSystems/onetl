<a id="greenplum-execute"></a>

# Executing statements in Greenplum

#### WARNING
Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

Do **NOT** use them to read large amounts of data. Use [DBReader](read.md#greenplum-read) instead.

## How to

There are 2 ways to execute some statement in Greenplum

### Use `Greenplum.fetch`

Use this method to perform some `SELECT` query which returns **small number or rows**, like reading
Greenplum config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts `Greenplum.FetchOptions`.

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### WARNING
`Greenplum.fetch` is implemented using Postgres JDBC connection,
so types are handled a bit differently than in `DBReader`. See [Postgres <-> Spark type mapping](../postgres/types.md#postgres-types).

#### Syntax support

This method supports **any** query syntax supported by Greenplum, like:

* ✅︎ `SELECT ... FROM ...`
* ✅︎ `WITH alias AS (...) SELECT ...`
* ✅︎ `SELECT func(arg1, arg2)` or `{call func(arg1, arg2)}` - special syntax for calling functions
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples

```python
from onetl.connection import Greenplum

greenplum = Greenplum(...)

df = greenplum.fetch(
    "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
    options=Greenplum.FetchOptions(queryTimeout=10),
)
greenplum.close()
value = df.collect()[0][0]  # get value from first row and first column
```

### Use `Greenplum.execute`

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts `Greenplum.ExecuteOptions`.

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### Syntax support

This method supports **any** query syntax supported by Greenplum, like:

* ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`, and so on
* ✅︎ `ALTER ...`
* ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...`, and so on
* ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE`, and so on
* ✅︎ `CALL procedure(arg1, arg2) ...`
* ✅︎ `SELECT func(arg1, arg2)` or `{call func(arg1, arg2)}` - special syntax for calling functions
* ✅︎ other statements not mentioned here
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples

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

## Interaction schema

Unlike reading & writing, executing statements in Greenplum is done **only** through Greenplum master node,
without any interaction between Greenplum segments and Spark executors. More than that, Spark executors are not used in this case.

The only port used while interacting with Greenplum in this case is `5432` (Greenplum master port).

### Spark <-> Greenplum interaction during Greenplum.execute()/Greenplum.fetch()

## Options
