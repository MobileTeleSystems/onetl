<a id="oracle-write"></a>

# Writing to Oracle using `DBWriter`

For writing data to Oracle, use `DBWriter`.

#### WARNING
Please take into account [Oracle <-> Spark type mapping](types.md#oracle-types)

#### WARNING
It is always recommended to create table explicitly using [Oracle.execute](execute.md#oracle-execute)
instead of relying on Spark’s table DDL generation.

This is because Spark’s DDL generator can create columns with different precision and types than it is expected,
causing precision loss or other issues.

## Examples

```python
from onetl.connection import Oracle
from onetl.db import DBWriter

oracle = Oracle(...)

df = ...  # data is here

writer = DBWriter(
    connection=oracle,
    target="schema.table",
    options=Oracle.WriteOptions(if_exists="append"),
)

writer.run(df)
```

## Options

Method above accepts  `OracleWriteOptions`
