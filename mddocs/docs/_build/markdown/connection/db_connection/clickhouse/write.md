<a id="clickhouse-write"></a>

# Writing to Clickhouse using `DBWriter`

For writing data to Clickhouse, use `DBWriter`.

#### WARNING
Please take into account [Clickhouse <-> Spark type mapping](types.md#clickhouse-types)

#### WARNING
It is always recommended to create table explicitly using [Clickhouse.execute](execute.md#clickhouse-execute)
instead of relying on Spark’s table DDL generation.

This is because Spark’s DDL generator can create columns with different precision and types than it is expected,
causing precision loss or other issues.

## Examples

```python
from onetl.connection import Clickhouse
from onetl.db import DBWriter

clickhouse = Clickhouse(...)

df = ...  # data is here

writer = DBWriter(
    connection=clickhouse,
    target="schema.table",
    options=Clickhouse.WriteOptions(
        if_exists="append",
        # ENGINE is required by Clickhouse
        createTableOptions="ENGINE = MergeTree() ORDER BY id",
    ),
)

writer.run(df)
```

## Options

Method above accepts  `Clickhouse.WriteOptions`
