<a id="teradata-execute"></a>

# Executing statements in Teradata

#### WARNING
Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

Do **NOT** use them to read large amounts of data. Use [DBReader](read.md#teradata-read) or [Teradata.sql](sql.md#teradata-sql) instead.

## How to

There are 2 ways to execute some statement in Teradata

### Use `Teradata.fetch`

Use this method to execute some `SELECT` query which returns **small number or rows**, like reading
Teradata config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts [`Teradata.FetchOptions`](#onetl.connection.db_connection.teradata.options.TeradataFetchOptions).

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### Syntax support

This method supports **any** query syntax supported by Teradata, like:

* ✅︎ `SELECT ... FROM ...`
* ✅︎ `WITH alias AS (...) SELECT ...`
* ✅︎ `SHOW ...`
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples

```python
from onetl.connection import Teradata

teradata = Teradata(...)

df = teradata.fetch(
    "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
    options=Teradata.FetchOptions(queryTimeout=10),
)
teradata.close()
value = df.collect()[0][0]  # get value from first row and first column
```

### Use `Teradata.execute`

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts [`Teradata.ExecuteOptions`](#onetl.connection.db_connection.teradata.options.TeradataExecuteOptions).

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### Syntax support

This method supports **any** query syntax supported by Teradata, like:

* ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`, and so on
* ✅︎ `ALTER ...`
* ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...`, and so on
* ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, `TRUNCATE TABLE`, and so on
* ✅︎ `CALL procedure(arg1, arg2) ...` or `{call procedure(arg1, arg2)}` - special syntax for calling procedure
* ✅︎ `EXECUTE macro(arg1, arg2)`
* ✅︎ `EXECUTE FUNCTION ...`
* ✅︎ other statements not mentioned here
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples

```python
from onetl.connection import Teradata

teradata = Teradata(...)

teradata.execute("DROP TABLE database.table")
teradata.execute(
    """
    CREATE MULTISET TABLE database.table AS (
        id BIGINT,
        key VARCHAR,
        value REAL
    )
    NO PRIMARY INDEX
    """,
    options=Teradata.ExecuteOptions(queryTimeout=10),
)
```

## Options

### *pydantic model* onetl.connection.db_connection.teradata.options.TeradataFetchOptions

Options related to fetching data from databases via JDBC.

#### Versionadded
Added in version 0.11.0: Replace `Teradata.JDBCOptions` → `Teradata.FetchOptions`

### Examples

#### NOTE
You can pass any value supported by underlying JDBC driver class,
even if it is not mentioned in this documentation.

```python
from onetl.connection import Teradata

options = Teradata.FetchOptions(
    queryTimeout=60_000,
    fetchsize=100_000,
    customSparkOption="value",
)
```

<!-- !! processed by numpydoc !! -->
* **Fields:**
  - [`fetchsize (int | None)`](#onetl.connection.db_connection.teradata.options.TeradataFetchOptions.fetchsize)
  - [`query_timeout (int | None)`](#onetl.connection.db_connection.teradata.options.TeradataFetchOptions.query_timeout)

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

### *pydantic model* onetl.connection.db_connection.teradata.options.TeradataExecuteOptions

Options related to executing statements in databases via JDBC.

#### Versionadded
Added in version 0.11.0: Replace `Teradata.JDBCOptions` → `Teradata.ExecuteOptions`

### Examples

#### NOTE
You can pass any value supported by underlying JDBC driver class,
even if it is not mentioned in this documentation.

```python
from onetl.connection import Teradata

options = Teradata.ExecuteOptions(
    queryTimeout=60_000,
    customSparkOption="value",
)
```

<!-- !! processed by numpydoc !! -->
* **Fields:**
  - [`fetchsize (int | None)`](#onetl.connection.db_connection.teradata.options.TeradataExecuteOptions.fetchsize)
  - [`query_timeout (int | None)`](#onetl.connection.db_connection.teradata.options.TeradataExecuteOptions.query_timeout)

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
