<a id="clickhouse-execute"></a>

# Executing statements in Clickhouse

#### WARNING
Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

Do **NOT** use them to read large amounts of data. Use [DBReader](read.md#clickhouse-read) or [Clickhouse.sql](sql.md#clickhouse-sql) instead.

## How to

There are 2 ways to execute some statement in Clickhouse

### Use `Clickhouse.fetch`

Use this method to perform some `SELECT` query which returns **small number or rows**, like reading
Clickhouse config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts `Clickhouse.FetchOptions`.

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### WARNING
Please take into account [Clickhouse <-> Spark type mapping](types.md#clickhouse-types).

#### Syntax support

This method supports **any** query syntax supported by Clickhouse, like:

* ✅︎ `SELECT ... FROM ...`
* ✅︎ `WITH alias AS (...) SELECT ...`
* ✅︎ `SELECT func(arg1, arg2)` - call function
* ✅︎ `SHOW ...`
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples

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

### Use `Clickhouse.execute`

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts `Clickhouse.ExecuteOptions`.

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### Syntax support

This method supports **any** query syntax supported by Clickhouse, like:

* ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`, and so on
* ✅︎ `ALTER ...`
* ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...`, and so on
* ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE`, and so on
* ✅︎ other statements not mentioned here
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples

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

## Notes

These methods **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

So it should **NOT** be used to read large amounts of data. Use [DBReader](read.md#clickhouse-read) or [Clickhouse.sql](sql.md#clickhouse-sql) instead.

## Options
