# Reading from Clickhouse using `DBReader` { #clickhouse-read }

[DBReader][db-reader] supports [strategy][strategy] for incremental data reading,
but does not support custom queries, like `JOIN`.

!!! warning

    Please take into account [Сlickhouse types][clickhouse-types]


## Supported DBReader features

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, supported strategies:
- - ✅︎ [Snapshot strategy][snapshot-strategy]
- - ✅︎ [Incremental strategy][incremental-strategy]
- - ✅︎ [Snapshot batch strategy][snapshot-batch-strategy]
- - ✅︎ [Incremental batch strategy][incremental-batch-strategy]
- ❌ `hint` (is not supported by Clickhouse)
- ❌ `df_schema`
- ✅︎ `options` (see [Clickhouse.ReadOptions][onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions])

## Examples

Snapshot strategy:

```python
from onetl.connection import Clickhouse
from onetl.db import DBReader

clickhouse = Clickhouse(...)

reader = DBReader(
    connection=clickhouse,
    source="schema.table",
    columns=["id", "key", "CAST(value AS String) value", "updated_dt"],
    where="key = 'something'",
    options=Clickhouse.ReadOptions(partitionColumn="id", numPartitions=10),
)
df = reader.run()
```

Incremental strategy:

```python
from onetl.connection import Clickhouse
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

clickhouse = Clickhouse(...)

reader = DBReader(
    connection=clickhouse,
    source="schema.table",
    columns=["id", "key", "CAST(value AS String) value", "updated_dt"],
    where="key = 'something'",
    hwm=DBReader.AutoDetectHWM(name="clickhouse_hwm", expression="updated_dt"),
    options=Clickhouse.ReadOptions(partitionColumn="id", numPartitions=10),
)

with IncrementalStrategy():
    df = reader.run()
```

## Recommendations

### Select only required columns

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names. This reduces the amount of data passed from Clickhouse to Spark.

### Pay attention to `where` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where="column = 'value'")` clause.
This both reduces the amount of data send from Clickhouse to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options { #clickhouse-read-options }

<!-- 
```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.clickhouse.options
```

```{eval-rst}
.. autopydantic_model:: ClickhouseReadOptions
    :inherited-members: GenericOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
```
 -->

::: onetl.connection.db_connection.clickhouse.options.ClickhouseReadOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true


