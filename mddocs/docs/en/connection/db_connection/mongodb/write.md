# Writing to MongoDB using `DBWriter` { #mongodb-write }

For writing data to MongoDB, use [DBWriter][db-writer].

!!! warning

    Please take into account [MongoDB types][mongodb-types]

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

Method above accepts [MongoDB.WriteOptions][onetl.connection.db_connection.mongodb.options.MongoDBWriteOptions]

<!-- 

```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.mongodb.options
```

```{eval-rst}
.. autopydantic_model:: MongoDBWriteOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
```

 -->

::: onetl.connection.db_connection.mongodb.options.MongoDBWriteOptions
    options:
        heading_level: 3
        show_root_heading: true
