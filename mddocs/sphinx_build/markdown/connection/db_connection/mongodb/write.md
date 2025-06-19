<a id="mongodb-write"></a>

# Writing to MongoDB using `DBWriter`

For writing data to MongoDB, use `DBWriter`.

#### WARNING
Please take into account [MongoDB <-> Spark type mapping](types.md#mongodb-types)

## Examples

```python
from onetl.connection import MongoDB
from onetl.db import DBWriter

mongodb = MongoDB(...)

df = ...  # data is here

writer = DBWriter(
    connection=mongodb,
    target="schema.table",
    options=MongoDB.WriteOptions(
        if_exists="append",
    ),
)

writer.run(df)
```

## Write options

Method above accepts  `MongoDB.WriteOptions`
