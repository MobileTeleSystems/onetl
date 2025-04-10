<a id="mysql-execute"></a>

# Executing statements in MySQL

#### WARNING
Methods below **read all the rows** returned from DB **to Spark driver memory**, and then convert them to DataFrame.

Do **NOT** use them to read large amounts of data. Use [DBReader](read.md#mysql-read) or [MySQL.sql](sql.md#mysql-sql) instead.

## How to

There are 2 ways to execute some statement in MySQL

### Use `MySQL.fetch`

Use this method to perform some `SELECT` query which returns **small number or rows**, like reading
MySQL config, or reading data from some reference table. Method returns Spark DataFrame.

Method accepts [`MySQL.FetchOptions`](#onetl.connection.db_connection.mysql.options.MySQLFetchOptions).

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### WARNING
Please take into account [MySQL <-> Spark type mapping](types.md#mysql-types).

#### Syntax support

This method supports **any** query syntax supported by MySQL, like:

* ✅︎ `SELECT ... FROM ...`
* ✅︎ `WITH alias AS (...) SELECT ...`
* ✅︎ `SELECT func(arg1, arg2)` or `{?= call func(arg1, arg2)}` - special syntax for calling function
* ✅︎ `SHOW ...`
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples

```python
from onetl.connection import MySQL

mysql = MySQL(...)

df = mysql.fetch(
    "SELECT value FROM some.reference_table WHERE key = 'some_constant'",
    options=MySQL.FetchOptions(queryTimeout=10),
)
mysql.close()
value = df.collect()[0][0]  # get value from first row and first column
```

### Use `MySQL.execute`

Use this method to execute DDL and DML operations. Each method call runs operation in a separated transaction, and then commits it.

Method accepts [`MySQL.ExecuteOptions`](#onetl.connection.db_connection.mysql.options.MySQLExecuteOptions).

Connection opened using this method should be then closed with `connection.close()` or `with connection:`.

#### Syntax support

This method supports **any** query syntax supported by MySQL, like:

* ✅︎ `CREATE TABLE ...`, `CREATE VIEW ...`, and so on
* ✅︎ `ALTER ...`
* ✅︎ `INSERT INTO ... SELECT ...`, `UPDATE ...`, `DELETE ...`, and so on
* ✅︎ `DROP TABLE ...`, `DROP VIEW ...`, and so on
* ✅︎ `CALL procedure(arg1, arg2) ...` or `{call procedure(arg1, arg2)}` - special syntax for calling procedure
* ✅︎ other statements not mentioned here
* ❌ `SET ...; SELECT ...;` - multiple statements not supported

#### Examples

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

## Options

### *pydantic model* onetl.connection.db_connection.mysql.options.MySQLFetchOptions

Options related to fetching data from databases via JDBC.

#### Versionadded
Added in version 0.11.0: Replace `MySQL.JDBCOptions` → `MySQL.FetchOptions`

### Examples

#### NOTE
You can pass any value supported by underlying JDBC driver class,
even if it is not mentioned in this documentation.

```python
from onetl.connection import MySQL

options = MySQL.FetchOptions(
    queryTimeout=60_000,
    fetchsize=100_000,
    customSparkOption="value",
)
```

<!-- !! processed by numpydoc !! -->
* **Fields:**
  - [`fetchsize (int | None)`](#onetl.connection.db_connection.mysql.options.MySQLFetchOptions.fetchsize)
  - [`query_timeout (int | None)`](#onetl.connection.db_connection.mysql.options.MySQLFetchOptions.query_timeout)

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

### *pydantic model* onetl.connection.db_connection.mysql.options.MySQLExecuteOptions

Options related to executing statements in databases via JDBC.

#### Versionadded
Added in version 0.11.0: Replace `MySQL.JDBCOptions` → `MySQL.ExecuteOptions`

### Examples

#### NOTE
You can pass any value supported by underlying JDBC driver class,
even if it is not mentioned in this documentation.

```python
from onetl.connection import MySQL

options = MySQL.ExecuteOptions(
    queryTimeout=60_000,
    customSparkOption="value",
)
```

<!-- !! processed by numpydoc !! -->
* **Fields:**
  - [`fetchsize (int | None)`](#onetl.connection.db_connection.mysql.options.MySQLExecuteOptions.fetchsize)
  - [`query_timeout (int | None)`](#onetl.connection.db_connection.mysql.options.MySQLExecuteOptions.query_timeout)

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
