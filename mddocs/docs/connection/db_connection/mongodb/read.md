# Reading from MongoDB using `DBReader` { #DBR-onetl-connection-db-connection-mongodb-read-reading-from-mongodb-using-dbreader }

[DBReader][DBR-onetl-db-reader] supports [strategy][DBR-onetl-strategy-read-strategies] for incremental data reading, but does not support custom pipelines, e.g. aggregation.

!!! warning

    Please take into account [MongoDB types][DBR-onetl-connection-db-connection-mongodb-types-mongodb-spark-type-mapping]

## Supported DBReader features { #DBR-onetl-connection-db-connection-mongodb-read-supported-dbreader-features }

- ❌ `columns` (for now, all document fields are read)
- ✅︎ `where` (passed to `{"$match": ...}` aggregation pipeline)
- ✅︎ `hwm`, supported strategies:
    - ✅︎ [Snapshot strategy][DBR-onetl-strategy-snapshot-strategy]
    - ✅︎ [Incremental strategy][DBR-onetl-connection-db-connection-clickhouse-read-incremental-strategy]
    - ✅︎ [Snapshot batch strategy][DBR-onetl-strategy-snapshot-batch-strategy]
    - ✅︎ [Incremental batch strategy][DBR-onetl-strategy-incremental-batch-strategy]
    - Note that `expression` field of HWM can only be a field name, not a custom expression
- ✅︎ `hint` (see [official documentation](https://www.mongodb.com/docs/v5.0/reference/operator/meta/hint/))
- ✅︎ `df_schema` (mandatory)
- ✅︎ `options` (see [MongoDB.ReadOptions][onetl.connection.db_connection.mongodb.options.MongoDBReadOptions])

## Examples { #DBR-onetl-connection-db-connection-mongodb-read-examples }

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

## Recommendations { #DBR-onetl-connection-db-connection-mongodb-read-recommendations }

### Pay attention to `where` value { #DBR-onetl-connection-db-connection-mongodb-read-pay-attention-to-where-value }

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `DBReader(where={"column": {"$eq": "value"}})` clause.
This both reduces the amount of data send from MongoDB to Spark, and may also improve performance of the query.
Especially if there are indexes for columns used in `where` clause.

## Read options { #DBR-onetl-connection-db-connection-mongodb-read-options }


::: onetl.connection.db_connection.mongodb.options.MongoDBReadOptions
