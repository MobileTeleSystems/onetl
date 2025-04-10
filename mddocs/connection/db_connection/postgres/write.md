<a id="postgres-write"></a>

# Writing to Postgres using `DBWriter`

For writing data to Postgres, use [`DBWriter`](../../../db/db_writer.md#onetl.db.db_writer.db_writer.DBWriter).

#### WARNING
Please take into account [Postgres <-> Spark type mapping](types.md#postgres-types)

#### WARNING
It is always recommended to create table explicitly using [Postgres.execute](execute.md#postgres-execute)
instead of relying on Spark’s table DDL generation.

This is because Spark’s DDL generator can create columns with different precision and types than it is expected,
causing precision loss or other issues.

## Examples

```python
from onetl.connection import Postgres
from onetl.db import DBWriter

postgres = Postgres(...)

df = ...  # data is here

writer = DBWriter(
    connection=postgres,
    target="schema.table",
    options=Postgres.WriteOptions(if_exists="append"),
)

writer.run(df)
```

## Options

Method above accepts  [`Postgres.WriteOptions`](#onetl.connection.db_connection.postgres.options.PostgresWriteOptions)

### *pydantic model* onetl.connection.db_connection.postgres.options.PostgresWriteOptions

Spark JDBC writing options.

#### Versionadded
Added in version 0.5.0: Replace `Postgres.Options` → `Postgres.WriteOptions`

### Examples

#### NOTE
You can pass any value
[supported by Spark](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html),
even if it is not mentioned in this documentation. **Option names should be in** `camelCase`!

The set of supported options depends on Spark version.

```python
from onetl.connection import Postgres

options = Postgres.WriteOptions(
    if_exists="append",
    batchsize=20_000,
    customSparkOption="value",
)
```

<!-- !! processed by numpydoc !! -->

#### *field* if_exists *: JDBCTableExistBehavior* *= JDBCTableExistBehavior.APPEND* *(alias 'mode')*

Behavior of writing data into existing table.

Possible values:
: * `append` (default)
    : Adds new rows into existing table.
      <br/>
      ### Behavior in details
      <br/>
      * Table does not exist
        : Table is created using options provided by user
          (`createTableOptions`, `createTableColumnTypes`, etc).
      * Table exists
        : Data is appended to a table. Table has the same DDL as before writing data
          <br/>
          #### WARNING
          This mode does not check whether table already contains
          rows from dataframe, so duplicated rows can be created.
          <br/>
          Also Spark does not support passing custom options to
          insert statement, like `ON CONFLICT`, so don’t try to
          implement deduplication using unique indexes or constraints.
          <br/>
          Instead, write to staging table and perform deduplication
          using `execute` method.
  * `replace_entire_table`
    : **Table is dropped and then created, or truncated**.
      <br/>
      ### Behavior in details
      <br/>
      * Table does not exist
        : Table is created using options provided by user
          (`createTableOptions`, `createTableColumnTypes`, etc).
      * Table exists
        : Table content is replaced with dataframe content.
          <br/>
          After writing completed, target table could either have the same DDL as
          before writing data (`truncate=True`), or can be recreated (`truncate=False`
          or source does not support truncation).
  * `ignore`
    : Ignores the write operation if the table already exists.
      <br/>
      ### Behavior in details
      <br/>
      * Table does not exist
        : Table is created using options provided by user
          (`createTableOptions`, `createTableColumnTypes`, etc).
      * Table exists
        : The write operation is ignored, and no data is written to the table.
  * `error`
    : Raises an error if the table already exists.
      <br/>
      ### Behavior in details
      <br/>
      * Table does not exist
        : Table is created using options provided by user
          (`createTableOptions`, `createTableColumnTypes`, etc).
      * Table exists
        : An error is raised, and no data is written to the table.

#### Versionchanged
Changed in version 0.9.0: Renamed `mode` → `if_exists`

<!-- !! processed by numpydoc !! -->

#### *field* query_timeout *: int | None* *= None* *(alias 'queryTimeout')*

The number of seconds the driver will wait for a statement to execute.
Zero means there is no limit.

This option depends on driver implementation,
some drivers can check the timeout of each query instead of an entire JDBC batch.

<!-- !! processed by numpydoc !! -->

#### *field* batchsize *: int* *= 20000*

How many rows can be inserted per round trip.

Tuning this option can influence performance of writing.

#### WARNING
Default value is different from Spark.

Spark uses quite small value `1000`, which is absolutely not usable
in BigData world.

Thus we’ve overridden default value with `20_000`,
which should increase writing performance.

You can increase it even more, up to `50_000`,
but it depends on your database load and number of columns in the row.
Higher values does not increase performance.

#### Versionchanged
Changed in version 0.4.0: Changed default value from 1000 to 20_000

<!-- !! processed by numpydoc !! -->

#### *field* isolation_level *: str* *= 'READ_UNCOMMITTED'* *(alias 'isolationLevel')*

The transaction isolation level, which applies to current connection.

Possible values:
: * `NONE` (as string, not Python’s `None`)
  * `READ_COMMITTED`
  * `READ_UNCOMMITTED`
  * `REPEATABLE_READ`
  * `SERIALIZABLE`

Values correspond to transaction isolation levels defined by JDBC standard.
Please refer the documentation for
[java.sql.Connection](https://docs.oracle.com/javase/8/docs/api/java/sql/Connection.html).

<!-- !! processed by numpydoc !! -->
