# MongoDB <-> Spark type mapping { #DBR-onetl-connection-db-connection-mongodb-types-mongodb-spark-type-mapping }

!!! note

    The results below are valid for Spark 3.5.8, and may differ on other Spark versions.

## Type detection & casting { #DBR-onetl-connection-db-connection-mongodb-types-type-detection-casting }

Spark's DataFrames always have a `schema` which is a list of fields with corresponding Spark types. All operations on a field are performed using field type.

MongoDB is, by design, \_\_schemaless\_\_. So there are 2 ways how this can be handled:

- User provides DataFrame schema explicitly:

??? example

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

    df_schema = StructType(
        [
            StructField("_id", StringType()),
            StructField("some", StringType()),
            StructField(
                "field",
                StructType(
                    [
                        StructField("nested", IntegerType()),
                    ]
                ),
            ),
        ]
    )

    reader = DBReader(
        connection=mongodb,
        source="some_collection",
        df_schema=df_schema,
    )
    df = reader.run()

    # or

    df = mongodb.pipeline(
        collection="some_collection",
        df_schema=df_schema,
    )
    ```

- Rely on MongoDB connector schema infer:

    ```python
    df = mongodb.pipeline(collection="some_collection")
    ```

  In this case MongoDB connector read a sample of collection documents, and build DataFrame schema based on document fields and values.

It is highly recommended to pass `df_schema` explicitly, to avoid type conversion issues.

### References { #DBR-onetl-connection-db-connection-mongodb-types-references }

Here you can find source code with type conversions:

- [MongoDB -> Spark](https://github.com/mongodb/mongo-spark/blob/r10.7.0/src/main/java/com/mongodb/spark/sql/connector/schema/InferSchema.java#L214-L260)
- [Spark -> MongoDB](https://github.com/mongodb/mongo-spark/blob/r10.7.0/src/main/java/com/mongodb/spark/sql/connector/schema/RowToBsonDocumentConverter.java#L164-L278)

## Supported types { #DBR-onetl-connection-db-connection-mongodb-types-supported-types }

See [official documentation](https://www.mongodb.com/docs/manual/reference/bson-types/)

### Numeric types { #DBR-onetl-connection-db-connection-mongodb-types-numeric-types }

| MongoDB type (read) | Spark type                  | MongoDB type (write) |
|---------------------|-----------------------------|----------------------|
| `Decimal128`      | `DecimalType(P=34, S=32)` | `Decimal128`       |
| `-`<br/>`Double`               | `FloatType()`<br/>`DoubleType()`             | `Double`           |
| `-`<br/>`-`<br/>`Int32`               | `ByteType()`<br/>`ShortType()`<br/>`IntegerType()`              | `Int32`            |
| `Int64`           | `LongType()`              | `Int64`            |

### Temporal types { #DBR-onetl-connection-db-connection-mongodb-types-temporal-types }

| MongoDB type (read)    | Spark type                        | MongoDB type (write)    |
|------------------------|-----------------------------------|-------------------------|
| `-`                   | `DateType()`, days              | `Date`, milliseconds  |
| `Date`, milliseconds | `TimestampType()`, microseconds | `Date`, milliseconds, **precision loss** [^1]|
| `Timestamp`, seconds | `TimestampType()`, microseconds | `Date`, milliseconds  |
| `-`<br/>`-`                  | `TimestampNTZType()`<br/>`DayTimeIntervalType()`            | unsupported             |

!!! warning

    Note that types in MongoDB and Spark have different value ranges:


    | MongoDB type  | Min value                      | Max value                      | Spark type          | Min value                      | Max value                      |
    |---------------|--------------------------------|--------------------------------|---------------------|--------------------------------|--------------------------------|
    | `Date`<br/>`Timestamp`      | -290 million years<br/>`1970-01-01 00:00:00`             | 290 million years<br/>`2106-02-07 09:28:16`              | `TimestampType()` | `0001-01-01 00:00:00.000000` | `9999-12-31 23:59:59.999999` |

    So not all values can be read from MongoDB to Spark, and can written from Spark DataFrame to MongoDB.

    References:

    * [MongoDB Date type documentation](https://www.mongodb.com/docs/manual/reference/bson-types/#date)
    * [MongoDB Timestamp documentation](https://www.mongodb.com/docs/manual/reference/bson-types/#timestamps)
    * [Spark DateType documentation](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DateType.html)
    * [Spark TimestampType documentation](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/TimestampType.html)

[^1]: MongoDB `Date` type has precision up to milliseconds (`23:59:59.999`).
    Inserting data with microsecond precision (`23:59:59.999999`)
    will lead to **throwing away microseconds**.

### String types { #DBR-onetl-connection-db-connection-mongodb-types-string-types }

Note: fields of deprecated MongoDB type `Symbol` are excluded during read.

| MongoDB type (read) | Spark type       | MongoDB type (write) |
|---------------------|------------------|----------------------|
| `String`<br/>`Code`<br/>`RegExp`          | `StringType()` | `String`           |

### Binary types { #DBR-onetl-connection-db-connection-mongodb-types-binary-types }

| MongoDB type (read) | Spark type      | MongoDB type (write) |
| ------------------- | --------------- | -------------------- |
| `Boolean`           | `BooleanType()` | `Boolean`            |
| `Binary`            | `BinaryType()`  | `Binary`             |

### Struct types { #DBR-onetl-connection-db-connection-mongodb-types-struct-types }

| MongoDB type (read) | Spark type            | MongoDB type (write) |
|---------------------|-----------------------|----------------------|
| `Array[T]`        | `ArrayType(T)`      | `Array[T]`         |
| `Object[...]`<br/>`-`     | `StructType([...])`<br/>`MapType(...)` | `Object[...]`<br/>      |

### Special types { #DBR-onetl-connection-db-connection-mongodb-types-special-types }

| MongoDB type (read) | Spark type                                              | MongoDB type (write)                  |
|---------------------|---------------------------------------------------------|---------------------------------------|
| `ObjectId`<br/>`MaxKey`<br/>`MinKey`        | <br/><br/>`StringType()`                                        | <br/><br/>`String`                            |
| `Null`<br/>`Undefined`            | `NullType()`                                          | `Null`                              |
| `DBRef`           | `StructType([$ref: StringType(), $id: StringType()])` | `Object[$ref: String, $id: String]` |

## Explicit type cast { #DBR-onetl-connection-db-connection-mongodb-types-explicit-type-cast }

### `DBReader` { #DBR-onetl-connection-db-connection-mongodb-types-dbreader }

Currently it is not possible to cast field types using `DBReader`. But this can be done using `MongoDB.pipeline`.

### `MongoDB.pipeline` { #DBR-onetl-connection-db-connection-mongodb-types-mongodb-pipeline }

You can use `$project` aggregation to cast field types:

```python
from pyspark.sql.types import IntegerType, StructField, StructType

from onetl.connection import MongoDB
from onetl.db import DBReader

mongodb = MongoDB(...)

df = mongodb.pipeline(
    collection="my_collection",
    pipeline=[
        {
            "$project": {
                # convert unsupported_field to string
                "unsupported_field_str": {
                    "$convert": {
                        "input": "$unsupported_field",
                        "to": "string",
                    },
                },
                # skip unsupported_field from result
                "unsupported_field": 0,
            }
        }
    ],
)

# cast field content to proper Spark type
df = df.select(
    df.id,
    df.supported_field,
    # explicit cast
    df.unsupported_field_str.cast("integer").alias("parsed_integer"),
)
```

### `DBWriter` { #DBR-onetl-connection-db-connection-mongodb-types-dbwriter }

Convert dataframe field to string on Spark side, and then write it to MongoDB:

```python
df = df.select(
    df.id,
    df.unsupported_field.cast("string").alias("array_field_json"),
)

writer.run(df)
```
