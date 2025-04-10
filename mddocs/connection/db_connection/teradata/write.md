<a id="teradata-write"></a>

# Writing to Teradata using `DBWriter`

For writing data to Teradata, use [`DBWriter`](../../../db/db_writer.md#onetl.db.db_writer.db_writer.DBWriter).

#### WARNING
It is always recommended to create table explicitly using [Teradata.execute](execute.md#teradata-execute)
instead of relying on Spark’s table DDL generation.

This is because Spark’s DDL generator can create columns with different precision and types than it is expected,
causing precision loss or other issues.

## Examples

```python
from onetl.connection import Teradata
from onetl.db import DBWriter

teradata = Teradata(
    ...,
    extra={"TYPE": "FASTLOAD", "TMODE": "TERA"},
)

df = ...  # data is here

writer = DBWriter(
    connection=teradata,
    target="database.table",
    options=Teradata.WriteOptions(
        if_exists="append",
        # avoid creating SET table, use MULTISET
        createTableOptions="NO PRIMARY INDEX",
    ),
)

writer.run(df.repartition(1))
```

## Recommendations

### Number of connections

Teradata is not MVCC based, so write operations take exclusive lock on the entire table.
So **it is impossible to write data to Teradata table in multiple parallel connections**, no exceptions.

The only way to write to Teradata without making deadlocks is write dataframe with exactly 1 partition.

It can be implemented using `df.repartition(1)`:

```python
# do NOT use df.coalesce(1) as it can freeze
writer.run(df.repartition(1))
```

This moves all the data to just one Spark worker, so it may consume a lot of RAM. It is usually require to increase `spark.executor.memory` to handle this.

Another way is to write all dataframe partitions one-by-one:

```python
from pyspark.sql.functions import spark_partition_id

# get list of all partitions in the dataframe
partitions = sorted(df.select(spark_partition_id()).distinct().collect())

for partition in partitions:
    # get only part of data within this exact partition
    part_df = df.where(**partition.asDict()).coalesce(1)

    writer.run(part_df)
```

This require even data distribution for all partitions to avoid data skew and spikes of RAM consuming.

### Choosing connection type

Teradata supports several [different connection types](https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#BABFGFAF):
: * `TYPE=DEFAULT` - perform plain `INSERT` queries
  * `TYPE=FASTLOAD` - uses special FastLoad protocol for insert queries

It is always recommended to use `TYPE=FASTLOAD` because:
: * It provides higher performance
  * It properly handles inserting `NULL` values (`TYPE=DEFAULT` raises an exception)

But it can be used only during write, not read.

### Choosing transaction mode

Teradata supports [2 different transaction modes](https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#TMODESEC):
: * `TMODE=ANSI`
  * `TMODE=TERA`

Choosing one of the modes can alter connector behavior. For example:
: * Inserting data which exceeds table column length, like insert `CHAR(25)` to column with type `CHAR(24)`:
  * * `TMODE=ANSI` - raises exception
  * * `TMODE=TERA` - truncates input string to 24 symbols
  * Creating table using Spark:
  * * `TMODE=ANSI` - creates `MULTISET` table
  * * `TMODE=TERA` - creates `SET` table with `PRIMARY KEY` is a first column in dataframe.
      This can lead to slower insert time, because each row will be checked against a unique index.
      Fortunately, this can be disabled by passing custom `createTableOptions`.

## Options

Method above accepts  [`Teradata.WriteOptions`](#onetl.connection.db_connection.teradata.options.TeradataWriteOptions)

### *pydantic model* onetl.connection.db_connection.teradata.options.TeradataWriteOptions

Spark JDBC writing options.

#### Versionadded
Added in version 0.5.0: Replace `Teradata.Options` → `Teradata.WriteOptions`

### Examples

#### NOTE
You can pass any value
[supported by Spark](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html),
even if it is not mentioned in this documentation. **Option names should be in** `camelCase`!

The set of supported options depends on Spark version.

```python
from onetl.connection import Teradata

options = Teradata.WriteOptions(
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
