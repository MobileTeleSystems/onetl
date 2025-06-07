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

Method accepts [`Clickhouse.FetchOptions`](#onetl.connection.db_connection.clickhouse.options.ClickhouseFetchOptions).

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

Method accepts [`Clickhouse.ExecuteOptions`](#onetl.connection.db_connection.clickhouse.options.ClickhouseExecuteOptions).

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

### *pydantic model* onetl.connection.db_connection.clickhouse.options.ClickhouseFetchOptions

Options related to fetching data from databases via JDBC.

#### Versionadded
Added in version 0.11.0: Replace `Clickhouse.JDBCOptions` → `Clickhouse.FetchOptions`

### Examples

#### NOTE
You can pass any value supported by underlying JDBC driver class,
even if it is not mentioned in this documentation.

```python
from onetl.connection import Clickhouse

options = Clickhouse.FetchOptions(
    queryTimeout=60_000,
    fetchsize=100_000,
    customSparkOption="value",
)
```

<!-- !! processed by numpydoc !! -->

#### *field* query_timeout *: int | None* *= None* *(alias 'queryTimeout')*

The number of seconds the driver will wait for a statement to execute.
Zero means there is no limit.

This option depends on driver implementation,
some drivers can check the timeout of each query instead of an entire JDBC batch.

<!-- !! processed by numpydoc !! -->

#### *field* fetchsize *: int | None* *= None*

How many rows to fetch per round trip.

Tuning this option can influence performance of reading.

#### WARNING
Default value depends on driver. For example, Oracle has
default `fetchsize=10`.

<!-- !! processed by numpydoc !! -->

### *pydantic model* onetl.connection.db_connection.clickhouse.options.ClickhouseExecuteOptions

Options related to executing statements in databases via JDBC.

#### Versionadded
Added in version 0.11.0: Replace `Clickhouse.JDBCOptions` → `Clickhouse.ExecuteOptions`

### Examples

#### NOTE
You can pass any value supported by underlying JDBC driver class,
even if it is not mentioned in this documentation.

```python
from onetl.connection import Clickhouse

options = Clickhouse.ExecuteOptions(
    queryTimeout=60_000,
    customSparkOption="value",
)
```

<!-- !! processed by numpydoc !! -->

#### *field* query_timeout *: int | None* *= None* *(alias 'queryTimeout')*

The number of seconds the driver will wait for a statement to execute.
Zero means there is no limit.

This option depends on driver implementation,
some drivers can check the timeout of each query instead of an entire JDBC batch.

<!-- !! processed by numpydoc !! -->

#### *field* fetchsize *: int | None* *= None*

How many rows to fetch per round trip.

Tuning this option can influence performance of reading.

#### WARNING
Default value depends on driver. For example, Oracle has
default `fetchsize=10`.

<!-- !! processed by numpydoc !! -->
