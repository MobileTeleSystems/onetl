<a id="mssql-write"></a>

# Writing to MSSQL using `DBWriter`

For writing data to MSSQL, use `DBWriter`.

#### WARNING
Please take into account [MSSQL <-> Spark type mapping](types.md#mssql-types)

#### WARNING
It is always recommended to create table explicitly using [MSSQL.execute](execute.md#mssql-execute)
instead of relying on Spark’s table DDL generation.

This is because Spark’s DDL generator can create columns with different precision and types than it is expected,
causing precision loss or other issues.

## Examples

```python
from onetl.connection import MSSQL
from onetl.db import DBWriter

mssql = MSSQL(...)

df = ...  # data is here

writer = DBWriter(
    connection=mssql,
    target="schema.table",
    options=MSSQL.WriteOptions(if_exists="append"),
)

writer.run(df)
```

## Options

Method above accepts  `MSSQL.WriteOptions`
