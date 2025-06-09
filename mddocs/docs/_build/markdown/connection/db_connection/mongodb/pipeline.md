<a id="mongodb-sql"></a>

# Reading from MongoDB using `MongoDB.pipeline`

`MongoDB.sql` allows passing custom pipeline,
but does not support incremental strategies.

#### WARNING
Please take into account [MongoDB <-> Spark type mapping](types.md#mongodb-types)

## Recommendations

### Pay attention to `pipeline` value

Instead of filtering data on Spark side using `df.filter(df.column == 'value')` pass proper `mongodb.pipeline(..., pipeline={"$match": {"column": {"$eq": "value"}}})` value.
This both reduces the amount of data send from MongoDB to Spark, and may also improve performance of the query.
Especially if there are indexes for columns used in `pipeline` value.

## References
