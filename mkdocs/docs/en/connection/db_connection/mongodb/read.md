(mongodb-read)=

# Reading from MongoDB using `DBReader`

{obj}`DBReader <onetl.db.db_reader.db_reader.DBReader>` supports {ref}`strategy` for incremental data reading,
but does not support custom pipelines, e.g. aggregation.

```{eval-rst}
.. warning::

    Please take into account :ref:`mongodb-types`
```

## Supported DBReader features

- ❌ `columns` (for now, all document fields are read)
- ✅︎ `where` (passed to `{"$match": ...}` aggregation pipeline)
- ✅︎ `hwm`, supported strategies:
- - ✅︎ {ref}`snapshot-strategy`
- - ✅︎ {ref}`incremental-strategy`
- - ✅︎ {ref}`snapshot-batch-strategy`
- - ✅︎ {ref}`incremental-batch-strategy`
- - Note that `expression` field of HWM can only be a field name, not a custom expression
- ✅︎ `hint` (see [official documentation](https://www.mongodb.com/docs/v5.0/reference/operator/meta/hint/))
- ✅︎ `df_schema` (mandatory)
- ✅︎ `options` (see {obj}`MongoDB.ReadOptions <onetl.connection.db_connection.mongodb.options.MongoDBReadOptions>`)

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

```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.mongodb.options
```

```{eval-rst}
.. autopydantic_model:: MongoDBReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
```
