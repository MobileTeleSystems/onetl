<a id="oracle-execute"></a>

# Executing statements in Oracle

#### WARNING
Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

Do **NOT** use them to read large amounts of data. Use [DBReader](read.md#oracle-read) or [Oracle.sql](sql.md#oracle-sql) instead.

## How to

There are 2 ways to execute some statement in Oracle

### Use `Oracle.fetch`

Use this method to execute some `SELECT` query which returns **small number or rows**, like reading
Oracle config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts [`Oracle.FetchOptions`](#onetl.connection.db_connection.oracle.options.OracleFetchOptions).

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### WARNING
Please take into account [Oracle <-> Spark type mapping](types.md#oracle-types).

#### Syntax support

This method supports **any** query syntax supported by Oracle, like:

* ✅︎ `SELECT ... FROM ...`
* ✅︎ `WITH alias AS (...) SELECT ...`
* ✅︎ `SELECT func(arg1, arg2) FROM DUAL` - call function
* ✅︎ `SHOW ...`
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples

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

### Use `Oracle.execute`

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts [`Oracle.ExecuteOptions`](#onetl.connection.db_connection.oracle.options.OracleExecuteOptions).

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### Syntax support

This method supports **any** query syntax supported by Oracle, like:

* ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`
* ✅︎ `ALTER ...`
* ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...`, and so on
* ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE`, and so on
* ✅︎ `CALL procedure(arg1, arg2) ...` or `{call procedure(arg1, arg2)}` - special syntax for calling procedure
* ✅︎ `DECLARE ... BEGIN ... END` - execute PL/SQL statement
* ✅︎ other statements not mentioned here
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples

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

## Options

### *pydantic model* onetl.connection.db_connection.oracle.options.OracleFetchOptions

Options related to fetching data from databases via JDBC.

#### Versionadded
Added in version 0.11.0: Replace `Oracle.JDBCOptions` → `Oracle.FetchOptions`

### Examples

#### NOTE
You can pass any value supported by underlying JDBC driver class,
even if it is not mentioned in this documentation.

```python
from onetl.connection import Oracle

options = Oracle.FetchOptions(
    queryTimeout=60_000,
    fetchsize=100_000,
    customSparkOption="value",
)
```

<!-- !! processed by numpydoc !! -->
* **Fields:**
  - [`fetchsize (int | None)`](#onetl.connection.db_connection.oracle.options.OracleFetchOptions.fetchsize)
  - [`query_timeout (int | None)`](#onetl.connection.db_connection.oracle.options.OracleFetchOptions.query_timeout)

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

### *pydantic model* onetl.connection.db_connection.oracle.options.OracleExecuteOptions

Options related to executing statements in databases via JDBC.

#### Versionadded
Added in version 0.11.0: Replace `Oracle.JDBCOptions` → `Oracle.ExecuteOptions`

### Examples

#### NOTE
You can pass any value supported by underlying JDBC driver class,
even if it is not mentioned in this documentation.

```python
from onetl.connection import Oracle

options = Oracle.ExecuteOptions(
    queryTimeout=60_000,
    customSparkOption="value",
)
```

<!-- !! processed by numpydoc !! -->
* **Fields:**
  - [`fetchsize (int | None)`](#onetl.connection.db_connection.oracle.options.OracleExecuteOptions.fetchsize)
  - [`query_timeout (int | None)`](#onetl.connection.db_connection.oracle.options.OracleExecuteOptions.query_timeout)

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
