<a id="mongodb-write"></a>

# Writing to MongoDB using `DBWriter`

For writing data to MongoDB, use [`DBWriter`](../../../db/db_writer.md#onetl.db.db_writer.db_writer.DBWriter).

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

Method above accepts  [`MongoDB.WriteOptions`](#onetl.connection.db_connection.mongodb.options.MongoDBWriteOptions)

### *pydantic model* onetl.connection.db_connection.mongodb.options.MongoDBWriteOptions

Writing options for MongoDB connector.

#### WARNING
Options `uri`, `database`, `collection` are populated from connection attributes,
and cannot be overridden by the user in `WriteOptions` to avoid issues.

#### Versionadded
Added in version 0.7.0.

### Examples

#### NOTE
You can pass any value
[supported by connector](https://www.mongodb.com/docs/spark-connector/current/batch-mode/batch-write-config/),
even if it is not mentioned in this documentation. **Option names should be in** `camelCase`!

The set of supported options depends on connector version.

```python
from onetl.connection import MongoDB

options = MongoDB.WriteOptions(
    if_exists="append",
    sampleSize=500,
    localThreshold=20,
)
```

<!-- !! processed by numpydoc !! -->

#### *field* if_exists *: MongoDBCollectionExistBehavior* *= MongoDBCollectionExistBehavior.APPEND* *(alias 'mode')*

Behavior of writing data into existing collection.

Possible values:
: * `append` (default)
    : Adds new objects into existing collection.
      <br/>
      ### Behavior in details
      <br/>
      * Collection does not exist
        : Collection is created using options provided by user
          (`shardkey` and others).
      * Collection exists
        : Data is appended to a collection.
          <br/>
          #### WARNING
          This mode does not check whether collection already contains
          objects from dataframe, so duplicated objects can be created.
  * `replace_entire_collection`
    : **Collection is deleted and then created**.
      <br/>
      ### Behavior in details
      <br/>
      * Collection does not exist
        : Collection is created using options provided by user
          (`shardkey` and others).
      * Collection exists
        : Collection content is replaced with dataframe content.
  * `ignore`
    : Ignores the write operation if the collection already exists.
      <br/>
      ### Behavior in details
      <br/>
      * Collection does not exist
        : Collection is created using options provided by user
      * Collection exists
        : The write operation is ignored, and no data is written to the collection.
  * `error`
    : Raises an error if the collection already exists.
      <br/>
      ### Behavior in details
      <br/>
      * Collection does not exist
        : Collection is created using options provided by user
      * Collection exists
        : An error is raised, and no data is written to the collection.

#### Versionchanged
Changed in version 0.9.0: Renamed `mode` → `if_exists`

<!-- !! processed by numpydoc !! -->
