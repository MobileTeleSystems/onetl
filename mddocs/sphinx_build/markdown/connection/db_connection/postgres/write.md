<a id="postgres-write"></a>

# Writing to Postgres using `DBWriter`

For writing data to Postgres, use `DBWriter`.

#### WARNING
Please take into account [Postgres <-> Spark type mapping](types.md#postgres-types)

#### WARNING
It is always recommended to create table explicitly using [Postgres.execute](execute.md#postgres-execute)
instead of relying on Spark’s table DDL generation.

This is because Spark’s DDL generator can create columns with different precision and types than it is expected,
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

## Options

Method above accepts  `Postgres.WriteOptions`
