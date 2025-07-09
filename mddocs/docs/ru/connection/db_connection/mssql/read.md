# Reading from MSSQL using `DBReader` { #mssql-read }

[DBReader][db-reader] supports [strategy][strategy] for incremental data reading,
but does not support custom queries, like `JOIN`.

!!! warning

    Please take into account [MSSQL types][mssql-types]

## Supported DBReader features

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, supported strategies:
- - ✅︎ [Snapshot strategy][snapshot-strategy]
- - ✅︎ [Incremental strategy][incremental-strategy]
- - ✅︎ [Snapshot batch strategy][snapshot-batch-strategy]
- - ✅︎ [Incremental batch strategy][incremental-batch-strategy]
- ❌ `hint` (MSSQL does support hints, but DBReader not, at least for now)
- ❌ `df_schema`
- ✅︎ `options` (see [MSSQL.ReadOptions][onetl.connection.db_connection.mssql.options.MSSQLReadOptions])

## Examples

Snapshot strategy:

```python
from onetl.connection import MSSQL
from onetl.db import DBReader

mssql = MSSQL(...)

reader = DBReader(
    connection=mssql,
    source="schema.table",
    columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
    where="key = 'something'",
    options=MSSQL.ReadOptions(partitionColumn="id", numPartitions=10),
)
df = reader.run()
```

Incremental strategy:

```python
from onetl.connection import MSSQL
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

mssql = MSSQL(...)

reader = DBReader(
    connection=mssql,
    source="schema.table",
    columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
    where="key = 'something'",
    hwm=DBReader.AutoDetectHWM(name="mssql_hwm", expression="updated_dt"),
    options=MSSQL.ReadOptions(partitionColumn="id", numPartitions=10),
)

with IncrementalStrategy():
    df = reader.run()
```

## Recommendations

### Select only required columns

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names. This reduces the amount of data passed from MSSQL to Spark.

### Pay attention to `where` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where="column = 'value'")` clause.
This both reduces the amount of data send from MSSQL to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options

<!-- 
```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.mssql.options
```

```{eval-rst}
.. autopydantic_model:: MSSQLReadOptions
    :inherited-members: GenericOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
```
 -->

::: onetl.connection.db_connection.mssql.options.MSSQLReadOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true