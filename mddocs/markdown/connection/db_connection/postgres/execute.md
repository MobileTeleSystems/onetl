<a id="postgres-execute"></a>

# Executing statements in Postgres

#### WARNING
Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

Do **NOT** use them to read large amounts of data. Use [DBReader](read.md#postgres-read) or [Postgres.sql](sql.md#postgres-sql) instead.

## How to

There are 2 ways to execute some statement in Postgres

### Use `Postgres.fetch`

Use this method to execute some `SELECT` query which returns **small number or rows**, like reading
Postgres config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts [`Postgres.FetchOptions`](#onetl.connection.db_connection.postgres.options.PostgresFetchOptions).

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### WARNING
Please take into account [Postgres <-> Spark type mapping](types.md#postgres-types).

#### Syntax support

This method supports **any** query syntax supported by Postgres, like:

* ✅︎ `SELECT ... FROM ...`
* ✅︎ `WITH alias AS (...) SELECT ...`
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples

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

### Use `Postgres.execute`

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts [`Postgres.ExecuteOptions`](#onetl.connection.db_connection.postgres.options.PostgresExecuteOptions).

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### Syntax support

This method supports **any** query syntax supported by Postgres, like:

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

## Options

### *pydantic model* onetl.connection.db_connection.postgres.options.PostgresFetchOptions

Options related to fetching data from databases via JDBC.

#### Versionadded
Added in version 0.11.0: Replace `Postgres.JDBCOptions` → `Postgres.FetchOptions`

### Examples

#### NOTE
You can pass any value supported by underlying JDBC driver class,
even if it is not mentioned in this documentation.

```python
from onetl.connection import Postgres

options = Postgres.FetchOptions(
    queryTimeout=60_000,
    fetchsize=100_000,
    customSparkOption="value",
)
```

<!-- !! processed by numpydoc !! -->
* **Fields:**
  - [`fetchsize (int | None)`](#onetl.connection.db_connection.postgres.options.PostgresFetchOptions.fetchsize)
  - [`query_timeout (int | None)`](#onetl.connection.db_connection.postgres.options.PostgresFetchOptions.query_timeout)

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

### *pydantic model* onetl.connection.db_connection.postgres.options.PostgresExecuteOptions

Options related to executing statements in databases via JDBC.

#### Versionadded
Added in version 0.11.0: Replace `Postgres.JDBCOptions` → `Postgres.ExecuteOptions`

### Examples

#### NOTE
You can pass any value supported by underlying JDBC driver class,
even if it is not mentioned in this documentation.

```python
from onetl.connection import Postgres

options = Postgres.ExecuteOptions(
    queryTimeout=60_000,
    customSparkOption="value",
)
```

<!-- !! processed by numpydoc !! -->
* **Fields:**
  - [`fetchsize (int | None)`](#onetl.connection.db_connection.postgres.options.PostgresExecuteOptions.fetchsize)
  - [`query_timeout (int | None)`](#onetl.connection.db_connection.postgres.options.PostgresExecuteOptions.query_timeout)

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
