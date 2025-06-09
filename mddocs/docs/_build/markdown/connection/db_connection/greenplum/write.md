<a id="greenplum-write"></a>

# Writing to Greenplum using `DBWriter`

For writing data to Greenplum, use `DBWriter`
with `GreenplumWriteOptions`.

#### WARNING
Please take into account [Greenplum <-> Spark type mapping](types.md#greenplum-types).

#### WARNING
It is always recommended to create table explicitly using [Greenplum.execute](execute.md#greenplum-execute)
instead of relying on Spark’s table DDL generation.

This is because Spark’s DDL generator can create columns with different types than it is expected.

## Examples

```python
from onetl.connection import Greenplum
from onetl.db import DBWriter

greenplum = Greenplum(...)

df = ...  # data is here

writer = DBWriter(
    connection=greenplum,
    target="schema.table",
    options=Greenplum.WriteOptions(
        if_exists="append",
        # by default distribution is random
        distributedBy="id",
        # partitionBy is not supported
    ),
)

writer.run(df)
```

## Interaction schema

High-level schema is described in [Prerequisites](prerequisites.md#greenplum-prerequisites). You can find detailed interaction schema below.

### Spark <-> Greenplum interaction during DBWriter.run()

## Options
