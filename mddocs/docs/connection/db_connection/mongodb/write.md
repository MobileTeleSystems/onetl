# Writing to MongoDB using `DBWriter` { #DBR-onetl-connection-db-connection-mongodb-write-writing-to-mongodb-using-dbwriter }

For writing data to MongoDB, use [DBWriter][DBR-onetl-db-writer].

!!! warning

    Please take into account [MongoDB types][DBR-onetl-connection-db-connection-mongodb-types-mongodb-spark-type-mapping]

## Examples { #DBR-onetl-connection-db-connection-mongodb-write-examples }

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

## Write options { #DBR-onetl-connection-db-connection-mongodb-write-options }

Method above accepts [MongoDB.WriteOptions][onetl.connection.db_connection.mongodb.options.MongoDBWriteOptions]


::: onetl.connection.db_connection.mongodb.options.MongoDBWriteOptions
