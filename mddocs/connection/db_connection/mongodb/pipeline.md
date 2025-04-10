<a id="mongodb-sql"></a>

# Reading from MongoDB using `MongoDB.pipeline`

[`MongoDB.sql`](#onetl.connection.db_connection.mongodb.connection.MongoDB.pipeline) allows passing custom pipeline,
but does not support incremental strategies.

#### WARNING
Please take into account [MongoDB <-> Spark type mapping](types.md#mongodb-types)

## Recommendations

### Pay attention to `pipeline` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `mongodb.pipeline(..., pipeline={"$match": {"column": {"$eq": "value"}}})` value.
This both reduces the amount of data send from MongoDB to Spark, and may also improve performance of the query.
Especially if there are indexes for columns used in `pipeline` value.

## References

#### MongoDB.pipeline(collection: str, pipeline: dict | list[dict] | None = None, df_schema: StructType | None = None, options: [MongoDBPipelineOptions](#onetl.connection.db_connection.mongodb.options.MongoDBPipelineOptions) | dict | None = None)

Execute a pipeline for a specific collection, and return DataFrame. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

Almost like [Aggregation pipeline syntax](https://www.mongodb.com/docs/manual/core/aggregation-pipeline/)
in MongoDB:

```js
db.collection_name.aggregate([{"$match": ...}, {"$group": ...}])
```

but pipeline is executed on Spark executors, in a distributed way.

#### NOTE
This method does not support [Read Strategies](../../../strategy/index.md#strategy),
use [`DBReader`](../../../db/db_reader.md#onetl.db.db_reader.db_reader.DBReader) instead

#### Versionadded
Added in version 0.7.0.

* **Parameters:**
  **collection**
  : Collection name.

  **pipeline**
  : Pipeline containing a database query.
    See [Aggregation pipeline syntax](https://www.mongodb.com/docs/manual/core/aggregation-pipeline/).

  **df_schema**
  : Schema describing the resulting DataFrame.

  **options**
  : Additional pipeline options, see [`MongoDB.PipelineOptions`](#onetl.connection.db_connection.mongodb.options.MongoDBPipelineOptions).

### Examples

Get document with a specific `field` value:

```python
df = connection.pipeline(
    collection="collection_name",
    pipeline={"$match": {"field": {"$eq": 1}}},
)
```

Calculate aggregation and get result:

```python
df = connection.pipeline(
    collection="collection_name",
    pipeline={
        "$group": {
            "_id": 1,
            "min": {"$min": "$column_int"},
            "max": {"$max": "$column_int"},
        }
    },
)
```

Explicitly pass DataFrame schema:

```python
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

df_schema = StructType(
    [
        StructField("_id", StringType()),
        StructField("some_string", StringType()),
        StructField("some_int", IntegerType()),
        StructField("some_datetime", TimestampType()),
        StructField("some_float", DoubleType()),
    ],
)

df = connection.pipeline(
    collection="collection_name",
    df_schema=df_schema,
    pipeline={"$match": {"some_int": {"$gt": 999}}},
)
```

Pass additional options to pipeline execution:

```python
df = connection.pipeline(
    collection="collection_name",
    pipeline={"$match": {"field": {"$eq": 1}}},
    options=MongoDB.PipelineOptions(hint={"field": 1}),
)
```

<!-- !! processed by numpydoc !! -->

### *pydantic model* onetl.connection.db_connection.mongodb.options.MongoDBPipelineOptions

Aggregation pipeline options for MongoDB connector.

The only difference from [`MongoDB.ReadOptions`](read.md#onetl.connection.db_connection.mongodb.options.MongoDBReadOptions) that latter does not allow to pass the `hint` parameter.

#### WARNING
Options `uri`, `database`, `collection`, `pipeline` are populated from connection attributes,
and cannot be overridden by the user in `PipelineOptions` to avoid issues.

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

options = MongoDB.PipelineOptions(
    hint={"some_field": 1},
)
```

<!-- !! processed by numpydoc !! -->
