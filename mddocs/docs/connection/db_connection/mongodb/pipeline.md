# Reading from MongoDB using `MongoDB.pipeline` { #DBR-onetl-connection-db-connection-mongodb-pipeline-reading-from-mongodb-using-mongodb-pipeline }

[MongoDB.sql][onetl.connection.db_connection.mongodb.connection.MongoDB.pipeline] allows passing custom pipeline, but does not support incremental strategies.

!!! warning

    Please take into account [Mongodb types][DBR-onetl-connection-db-connection-mongodb-types-mongodb-spark-type-mapping]

## Recommendations { #DBR-onetl-connection-db-connection-mongodb-pipeline-recommendations }

### Pay attention to `pipeline` value { #DBR-onetl-connection-db-connection-mongodb-pipeline-pay-attention-to-pipeline-value }

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `mongodb.pipeline(..., pipeline={"$match": {"column": {"$eq": "value"}}})` value.
This both reduces the amount of data send from MongoDB to Spark, and may also improve performance of the query.
Especially if there are indexes for columns used in `pipeline` value.

## References { #DBR-onetl-connection-db-connection-mongodb-pipeline-references }


::: onetl.connection.db_connection.mongodb.connection.MongoDB.pipeline

::: onetl.connection.db_connection.mongodb.options.MongoDBPipelineOptions
