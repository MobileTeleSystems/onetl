<a id="kafka-write"></a>

# Writing to Kafka

For writing data to Kafka, use [`DBWriter`](../../../db/db_writer.md#onetl.db.db_writer.db_writer.DBWriter) with specific options (see below).

## Dataframe schema

Unlike other DB connections, Kafka does not have concept of columns.
All the topics messages have the same set of fields. Only some of them can be written:

```text
root
|-- key: binary (nullable = true)
|-- value: binary (nullable = true)
|-- headers: struct (nullable = true)
    |-- key: string (nullable = false)
    |-- value: binary (nullable = true)
```

`headers` can be passed only with `Kafka.WriteOptions(include_headers=True)` (compatibility with Kafka 1.x).

Field `topic` should not be present in the dataframe, as it is passed to `DBWriter(target=...)`.

Other fields, like `partition`, `offset`, `timestamp` are set by Kafka, and cannot be passed explicitly.

## Value serialization

To write `value` or `key` of other type than bytes (e.g. struct or integer), users have to serialize values manually.

This could be done using following methods:
: * [`Avro.serialize_column`](../../../file_df/file_formats/avro.md#onetl.file.format.avro.Avro.serialize_column)
  * [`JSON.serialize_column`](../../../file_df/file_formats/json.md#onetl.file.format.json.JSON.serialize_column)
  * [`CSV.serialize_column`](../../../file_df/file_formats/csv.md#onetl.file.format.csv.CSV.serialize_column)

## Examples

Convert `value` to JSON string, and write to Kafka:

```python
from onetl.connection import Kafka
from onetl.db import DBWriter
from onetl.file.format import JSON

df = ...  # original data is here

# serialize struct data as JSON
json = JSON()
write_df = df.select(
    df.key,
    json.serialize_column(df.value),
)

# write data to Kafka
kafka = Kafka(...)

writer = DBWriter(
    connection=kafka,
    target="topic_name",
)
writer.run(write_df)
```

## Options

### *pydantic model* onetl.connection.db_connection.kafka.options.KafkaWriteOptions

Writing options for Kafka connector.

#### WARNING
Options:
: * `kafka.*`
  * `topic`

are populated from connection attributes, and cannot be overridden by the user in `WriteOptions` to avoid issues.

#### Versionadded
Added in version 0.9.0.

### Examples

#### NOTE
You can pass any value
[supported by connector](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html),
even if it is not mentioned in this documentation. **Option names should be in** `camelCase`!

The set of supported options depends on connector version.

```python
from onetl.connection import Kafka

options = Kafka.WriteOptions(
    if_exists="append",
    includeHeaders=True,
)
```

<!-- !! processed by numpydoc !! -->

#### *field* if_exists *: KafkaTopicExistBehaviorKafka* *= KafkaTopicExistBehaviorKafka.APPEND*

Behavior of writing data into existing topic.

Same as `df.write.mode(...)`.

Possible values:
: * `append` (default) - Adds new objects into existing topic.
  * `error` - Raises an error if topic already exists.

<!-- !! processed by numpydoc !! -->

#### *field* include_headers *: bool* *= False* *(alias 'includeHeaders')*

If `True`, `headers` column from dataframe can be written to Kafka (requires Kafka 2.0+).

If `False` and dataframe contains `headers` column, an exception will be raised.

<!-- !! processed by numpydoc !! -->
