(postgres-read)=

# Reading from Postgres using `DBReader`

{obj}`DBReader <onetl.db.db_reader.db_reader.DBReader>` supports {ref}`strategy` for incremental data reading,
but does not support custom queries, like `JOIN`.

```{eval-rst}
.. warning::

    Please take into account :ref:`postgres-types`
```

## Supported DBReader features

- ✅︎ `columns`
- ✅︎ `where`
- ✅︎ `hwm`, supported strategies:
- - ✅︎ {ref}`snapshot-strategy`
- - ✅︎ {ref}`incremental-strategy`
- - ✅︎ {ref}`snapshot-batch-strategy`
- - ✅︎ {ref}`incremental-batch-strategy`
- ❌ `hint` (is not supported by Postgres)
- ❌ `df_schema`
- ✅︎ `options` (see {obj}`Postgres.ReadOptions <onetl.connection.db_connection.postgres.options.PostgresReadOptions>`)

## Examples

Snapshot strategy:

```python
from onetl.connection import Postgres
from onetl.db import DBReader

postgres = Postgres(...)

reader = DBReader(
    connection=postgres,
    source="schema.table",
    columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
    where="key = 'something'",
    options=Postgres.ReadOptions(partitionColumn="id", numPartitions=10),
)
df = reader.run()
```

Incremental strategy:

```python
from onetl.connection import Postgres
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

postgres = Postgres(...)

reader = DBReader(
    connection=postgres,
    source="schema.table",
    columns=["id", "key", "CAST(value AS text) value", "updated_dt"],
    where="key = 'something'",
    hwm=DBReader.AutoDetectHWM(name="postgres_hwm", expression="updated_dt"),
    options=Postgres.ReadOptions(partitionColumn="id", numPartitions=10),
)

with IncrementalStrategy():
    df = reader.run()
```

## Recommendations

### Select only required columns

Instead of passing `"*"` in `DBReader(columns=[...])` prefer passing exact column names. This reduces the amount of data passed from Postgres to Spark.

### Pay attention to `where` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where="column = 'value'")` clause.
This both reduces the amount of data send from Postgres to Spark, and may also improve performance of the query.
Especially if there are indexes or partitions for columns used in `where` clause.

## Options

```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.postgres.options
```

```{eval-rst}
.. autopydantic_model:: PostgresReadOptions
    :inherited-members: GenericOptions
    :member-order: bysource
```
