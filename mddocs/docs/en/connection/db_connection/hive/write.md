(hive-write)=

# Writing to Hive using `DBWriter`

For writing data to Hive, use {obj}`DBWriter <onetl.db.db_writer.db_writer.DBWriter>`.

## Examples

```python
from onetl.connection import Hive
from onetl.db import DBWriter

hive = Hive(...)

df = ...  # data is here

# Create dataframe with specific number of Spark partitions.
# Use the Hive partitioning columns to group the data. Create max 20 files per Hive partition.
# Also sort the data by column which most data is correlated with (e.g. user_id), reducing files size.

num_files_per_partition = 20
partition_columns = ["country", "business_date"]
sort_columns = ["user_id"]
write_df = df.repartition(
    num_files_per_partition,
    *partition_columns,
    *sort_columns,
).sortWithinPartitions(*partition_columns, *sort_columns)

writer = DBWriter(
    connection=hive,
    target="schema.table",
    options=Hive.WriteOptions(
        if_exists="append",
        # Hive partitioning columns.
        partitionBy=partition_columns,
    ),
)

writer.run(write_df)
```

## Recommendations

### Use column-based write formats

Prefer these write formats:
: - [ORC](https://spark.apache.org/docs/latest/sql-data-sources-orc.html) (**default**)
  - [Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
  - [Iceberg](https://iceberg.apache.org/spark-quickstart/)
  - [Hudi](https://hudi.apache.org/docs/quick-start-guide/)
  - [Delta](https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake)

```{eval-rst}
.. warning::
    When using ``DBWriter``, the default spark data format configured in ``spark.sql.sources.default`` is ignored, as  ``Hive.WriteOptions(format=...)`` default value is explicitly set to ``orc``.
```

For column-based write formats, each file contains separated sections where column data is stored. The file footer contains
location of each column section/group. Spark can use this information to load only sections required by specific query, e.g. only selected columns,
to drastically speed up the query.

Another advantage is high compression ratio, e.g. 10x-100x in comparison to JSON or CSV.

### Use partitioning

#### How does it work

Hive support splitting data to partitions, which are different directories in filesystem with names like `some_col=value1/another_col=value2`.

For example, dataframe with content like this:

| country: string | business_date: date | user_id: int | bytes: long |
| --------------- | ------------------- | ------------ | ----------- |
| RU              | 2024-01-01          | 1234         | 25325253525 |
| RU              | 2024-01-01          | 2345         | 23234535243 |
| RU              | 2024-01-02          | 1234         | 62346634564 |
| US              | 2024-01-01          | 5678         | 4252345354  |
| US              | 2024-01-02          | 5678         | 5474575745  |
| US              | 2024-01-03          | 5678         | 3464574567  |

With `partitionBy=["country", "business_dt"]` data will be stored as files in the following subfolders:
: - `/country=RU/business_date=2024-01-01/`
  - `/country=RU/business_date=2024-01-02/`
  - `/country=US/business_date=2024-01-01/`
  - `/country=US/business_date=2024-01-02/`
  - `/country=US/business_date=2024-01-03/`

A separated subdirectory is created for each distinct combination of column values in the dataframe.

Please do not confuse Spark dataframe partitions (a.k.a batches of data handled by Spark executors, usually in parallel)
and Hive partitioning (store data in different subdirectories).
Number of Spark dataframe partitions is correlated the number of files created in **each** Hive partition.
For example, Spark dataframe with 10 partitions and 5 distinct values of Hive partition columns will be saved as 5 subfolders with 10 files each = 50 files in total.
Without Hive partitioning, all the files are placed into one flat directory.

#### But why?

Queries which has `WHERE` clause with filters on Hive partitioning columns, like `WHERE country = 'RU' AND business_date='2024-01-01'`, will
read only files from this exact partitions, like `/country=RU/business_date=2024-01-01/`, and skip files from other partitions.

This drastically increases performance and reduces the amount of memory used by Spark.
Consider using Hive partitioning in all tables.

#### Which columns should I use?

Usually Hive partitioning columns are based on event date or location, like `country: string`, `business_date: date`, `run_date: date` and so on.

**Partition columns should contain data with low cardinality.**
Dates, small integers, strings with low number of possible values are OK.
But timestamp, float, decimals, longs (like user id), strings with lots oj unique values (like user name or email) should **NOT** be used as Hive partitioning columns.
Unlike some other databases, range and hash-based partitions are not supported.

Partition column should be a part of a dataframe. If you want to partition values by date component of `business_dt: timestamp` column,
add a new column to dataframe like this: `df.withColumn("business_date", date(df.business_dt))`.

### Use compression

Using compression algorithms like `snappy`, `lz4` or `zstd` can reduce the size of files (up to 10x).

### Prefer creating large files

Storing millions of small files is not that HDFS and S3 are designed for. Minimal file size should be at least 10Mb, but usually it is like 128Mb+ or 256Mb+ (HDFS block size).
**NEVER** create files with few Kbytes in size.

Number of files can be different in different cases.
On one hand, Spark Adaptive Query Execution (AQE) can merge small Spark dataframe partitions into one larger.
On the other hand, dataframes with skewed data can produce a larger number of files than expected.

To create small amount of large files, you can reduce number of Spark dataframe partitions.
Use [df.repartition(N, columns...)](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html) function,
like this: `df.repartition(20, "col1", "col2")`.
This creates new Spark dataframe with partitions using `hash(df.col1 + df.col2) mod 20` expression, avoiding data skew.

Note: larger dataframe partitions requires more resources (CPU, RAM) on Spark executor. The exact number of partitions
should be determined empirically, as it depends on the amount of data and available resources.

### Sort data before writing

Dataframe with sorted content:

| country: string | business_date: date | user_id: int | business_dt: timestamp  | bytes: long |
| --------------- | ------------------- | ------------ | ----------------------- | ----------- |
| RU              | 2024-01-01          | 1234         | 2024-01-01T11:22:33.456 | 25325253525 |
| RU              | 2024-01-01          | 1234         | 2024-01-01T12:23:44.567 | 25325253525 |
| RU              | 2024-01-02          | 1234         | 2024-01-01T13:25:56.789 | 34335645635 |
| US              | 2024-01-01          | 2345         | 2024-01-01T10:00:00.000 | 12341       |
| US              | 2024-01-02          | 2345         | 2024-01-01T15:11:22.345 | 13435       |
| US              | 2024-01-03          | 2345         | 2024-01-01T20:22:33.567 | 14564       |

Has a much better compression rate than unsorted one, e.g. 2x or even higher:

| country: string | business_date: date | user_id: int | business_dt: timestamp  | bytes: long |
| --------------- | ------------------- | ------------ | ----------------------- | ----------- |
| RU              | 2024-01-01          | 1234         | 2024-01-01T11:22:33.456 | 25325253525 |
| RU              | 2024-01-01          | 6345         | 2024-12-01T23:03:44.567 | 25365       |
| RU              | 2024-01-02          | 5234         | 2024-07-01T06:10:56.789 | 45643456747 |
| US              | 2024-01-01          | 4582         | 2024-04-01T17:59:00.000 | 362546475   |
| US              | 2024-01-02          | 2345         | 2024-09-01T04:24:22.345 | 3235        |
| US              | 2024-01-03          | 3575         | 2024-03-01T21:37:33.567 | 346345764   |

Choosing columns to sort data by is really depends on the data. If data is correlated with some specific
column, like in example above the amount of traffic is correlated with both `user_id` and `timestamp`,
use `df.sortWithinPartitions("user_id", "timestamp")` before writing the data.

If `df.repartition(N, repartition_columns...)` is used in combination with `df.sortWithinPartitions(sort_columns...)`,
then `sort_columns` should start with `repartition_columns` or be equal to it.

## Options

```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.hive.options
```

```{eval-rst}
.. autopydantic_model:: HiveWriteOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
```
