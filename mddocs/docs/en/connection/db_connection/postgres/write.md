# Writing to Postgres using `DBWriter` { #postgres-write }

For writing data to Postgres, use [DBWriter][db-writer].

!!! warning

    Please take into account [Postgres types][postgres-types]

!!! warning

    It is always recommended to create table explicitly using [Postgres.execute][postgres-execute]
    instead of relying on Spark's table DDL generation.

    This is because Spark's DDL generator can create columns with different precision and types than it is expected,
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

## Options { #postgres-write-options }

Method above accepts [Postgres.WriteOptions][onetl.connection.db_connection.postgres.options.PostgresWriteOptions]

<!-- 
```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.postgres.options
```

```{eval-rst}
.. autopydantic_model:: PostgresWriteOptions
    :inherited-members: GenericOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
```
 -->

::: onetl.connection.db_connection.postgres.options.PostgresWriteOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true
