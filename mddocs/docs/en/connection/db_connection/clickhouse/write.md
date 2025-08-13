# Writing to Clickhouse using `DBWriter` { #clickhouse-write }

For writing data to Clickhouse, use [DBWriter][db-writer].

!!! warning

    Please take into account [Clickhouse types][clickhouse-types]


!!! warning

    It is always recommended to create table explicitly using [Clickhouse.execute][clickhouse-execute]
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected,
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

## Options { #clickhouse-write-options }

Method above accepts [Clickhouse.WriteOptions][onetl.connection.db_connection.clickhouse.options.ClickhouseWriteOptions]

<!-- 
```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.clickhouse.options
```

```{eval-rst}
.. autopydantic_model:: ClickhouseWriteOptions
    :inherited-members: GenericOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
``` 
-->

::: onetl.connection.db_connection.clickhouse.options.ClickhouseWriteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
