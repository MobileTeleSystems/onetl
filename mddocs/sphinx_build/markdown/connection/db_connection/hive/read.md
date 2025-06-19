<a id="hive-read"></a>

# Reading from Hive using `DBReader`

`DBReader` supports [Read Strategies](../../../strategy/index.md#strategy) for incremental data reading,
but does not support custom queries, like `JOIN`.

## Supported DBReader features

* ✅︎ `columns`
* ✅︎ `where`
* ✅︎ `hwm`, supported strategies:
* * ✅︎ [Snapshot Strategy](../../../strategy/snapshot_strategy.md#snapshot-strategy)
* * ✅︎ [Incremental Strategy](../../../strategy/incremental_strategy.md#incremental-strategy)
* * ✅︎ [Snapshot Batch Strategy](../../../strategy/snapshot_batch_strategy.md#snapshot-batch-strategy)
* * ✅︎ [Incremental Batch Strategy](../../../strategy/incremental_batch_strategy.md#incremental-batch-strategy)
* ❌ `hint` (is not supported by Hive)
* ❌ `df_schema`
* ❌ `options` (only Spark config params are used)

#### WARNING
Actually, `columns`, `where` and  `hwm.expression` should be written using [SparkSQL](https://spark.apache.org/docs/latest/sql-ref-syntax.html#data-retrieval-statements) syntax,
not HiveQL.

## Examples

Snapshot strategy:

```python
from onetl.connection import Hive
from onetl.db import DBReader

hive = Hive(...)

reader = DBReader(
    connection=hive,
    source="schema.table",
    columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
    where="key = 'something'",
)
df = reader.run()
```

Incremental strategy:

```python
from onetl.connection import Hive
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

hive = Hive(...)

reader = DBReader(
    connection=hive,
    source="schema.table",
    columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
    where="key = 'something'",
    hwm=DBReader.AutoDetectHWM(name="hive_hwm", expression="updated_dt"),
)

with IncrementalStrategy():
    df = reader.run()
```

## Recommendations

### Use column-based write formats

Prefer these write formats:
: * [ORC](https://spark.apache.org/docs/latest/sql-data-sources-orc.html)
  * [Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
  * [Iceberg](https://iceberg.apache.org/spark-quickstart/)
  * [Hudi](https://hudi.apache.org/docs/quick-start-guide/)
  * [Delta](https://docs.delta.io/latest/quick-start.html#set-up-apache-spark-with-delta-lake)

For colum-based write formats, each file contains separated sections there column data is stored. The file footer contains
location of each column section/group. Spark can use this information to load only sections required by specific query, e.g. only selected columns,
to drastically speed up the query.

Another advantage is high compression ratio, e.g. 10x-100x in comparison to JSON or CSV.

### Select only required columns

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names.
This drastically reduces the amount of data read by Spark, **if column-based file formats are used**.

### Use partition columns in `where` clause

Queries should include `WHERE` clause with filters on Hive partitioning columns.
This allows Spark to read only small set of files (*partition pruning*) instead of scanning the entire table, so this drastically increases performance.

Supported operators are: `=`, `>`, `<` and `BETWEEN`, and only against some **static** value.
