<a id="mongodb-read"></a>

# Reading from MongoDB using `DBReader`

[`DBReader`](../../../db/db_reader.md#onetl.db.db_reader.db_reader.DBReader) supports [Read Strategies](../../../strategy/index.md#strategy) for incremental data reading,
but does not support custom pipelines, e.g. aggregation.

#### WARNING
Please take into account [MongoDB <-> Spark type mapping](types.md#mongodb-types)

## Supported DBReader features

* ❌ `columns` (for now, all document fields are read)
* ✅︎ `where` (passed to `{"$match": ...}` aggregation pipeline)
* ✅︎ `hwm`, supported strategies:
* * ✅︎ [Snapshot Strategy](../../../strategy/snapshot_strategy.md#snapshot-strategy)
* * ✅︎ [Incremental Strategy](../../../strategy/incremental_strategy.md#incremental-strategy)
* * ✅︎ [Snapshot Batch Strategy](../../../strategy/snapshot_batch_strategy.md#snapshot-batch-strategy)
* * ✅︎ [Incremental Batch Strategy](../../../strategy/incremental_batch_strategy.md#incremental-batch-strategy)
* * Note that `expression` field of HWM can only be a field name, not a custom expression
* ✅︎ `hint` (see [official documentation](https://www.mongodb.com/docs/v5.0/reference/operator/meta/hint/))
* ✅︎ `df_schema` (mandatory)
* ✅︎ `options` (see [`MongoDB.ReadOptions`](#onetl.connection.db_connection.mongodb.options.MongoDBReadOptions))

## Examples

Snapshot strategy:

```python
from onetl.connection import MongoDB
from onetl.db import DBReader

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)

mongodb = MongoDB(...)

# mandatory
df_schema = StructType(
    [
        StructField("_id", StringType()),
        StructField("some", StringType()),
        StructField(
            "field",
            StructType(
                [
                    StructField("nested", IntegerType()),
                ],
            ),
        ),
        StructField("updated_dt", TimestampType()),
    ]
)

reader = DBReader(
    connection=mongodb,
    source="some_collection",
    df_schema=df_schema,
    where={"field": {"$eq": 123}},
    hint={"field": 1},
    options=MongoDBReadOptions(batchSize=10000),
)
df = reader.run()
```

Incremental strategy:

```python
from onetl.connection import MongoDB
from onetl.db import DBReader
from onetl.strategy import IncrementalStrategy

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)

mongodb = MongoDB(...)

# mandatory
df_schema = StructType(
    [
        StructField("_id", StringType()),
        StructField("some", StringType()),
        StructField(
            "field",
            StructType(
                [
                    StructField("nested", IntegerType()),
                ],
            ),
        ),
        StructField("updated_dt", TimestampType()),
    ]
)

reader = DBReader(
    connection=mongodb,
    source="some_collection",
    df_schema=df_schema,
    where={"field": {"$eq": 123}},
    hint={"field": 1},
    hwm=DBReader.AutoDetectHWM(name="mongodb_hwm", expression="updated_dt"),
    options=MongoDBReadOptions(batchSize=10000),
)

with IncrementalStrategy():
    df = reader.run()
```

## Recommendations

### Pay attention to `where` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where={"column": {"$eq": "value"}})` clause.
This both reduces the amount of data send from MongoDB to Spark, and may also improve performance of the query.
Especially if there are indexes for columns used in `where` clause.

## Read options

### *pydantic model* onetl.connection.db_connection.mongodb.options.MongoDBReadOptions

Reading options for MongoDB connector.

#### WARNING
Options `uri`, `database`, `collection`, `pipeline`, `hint` are populated from connection
attributes, and cannot be overridden by the user in `ReadOptions` to avoid issues.

#### Versionadded
Added in version 0.7.0.

### Examples

#### NOTE
You can pass any value
[supported by connector](https://www.mongodb.com/docs/spark-connector/current/batch-mode/batch-read-config/),
even if it is not mentioned in this documentation. **Option names should be in** `camelCase`!

The set of supported options depends on connector version.

```python
from onetl.connection import MongoDB

options = MongoDB.ReadOptions(
    sampleSize=100,
)
```

<!-- !! processed by numpydoc !! -->
