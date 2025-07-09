# Writing to Teradata using `DBWriter` { #teradata-write }

For writing data to Teradata, use [DBWriter][db-writer].

!!! warning

    It is always recommended to create table explicitly using :ref:`Teradata.execute <teradata-execute>`
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected,
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
: - `TYPE=DEFAULT` - perform plain `INSERT` queries
  - `TYPE=FASTLOAD` - uses special FastLoad protocol for insert queries

It is always recommended to use `TYPE=FASTLOAD` because:
: - It provides higher performance
  - It properly handles inserting `NULL` values (`TYPE=DEFAULT` raises an exception)

But it can be used only during write, not read.

### Choosing transaction mode

Teradata supports [2 different transaction modes](https://teradata-docs.s3.amazonaws.com/doc/connectivity/jdbc/reference/current/jdbcug_chapter_2.html#TMODESEC):
: - `TMODE=ANSI`
  - `TMODE=TERA`

Choosing one of the modes can alter connector behavior. For example:
: - Inserting data which exceeds table column length, like insert `CHAR(25)` to column with type `CHAR(24)`:
  - - `TMODE=ANSI` - raises exception
  - - `TMODE=TERA` - truncates input string to 24 symbols
  - Creating table using Spark:
  - - `TMODE=ANSI` - creates `MULTISET` table
  - - `TMODE=TERA` - creates `SET` table with `PRIMARY KEY` is a first column in dataframe.
      This can lead to slower insert time, because each row will be checked against a unique index.
      Fortunately, this can be disabled by passing custom `createTableOptions`.

## Options { #teradata-write-options }

Method above accepts [Teradata.WriteOptions][onetl.connection.db_connection.teradata.options.TeradataWriteOptions]

<!-- 
```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.teradata.options
```

```{eval-rst}
.. autopydantic_model:: TeradataWriteOptions
    :inherited-members: GenericOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
```
 -->

::: onetl.connection.db_connection.teradata.options.TeradataWriteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
