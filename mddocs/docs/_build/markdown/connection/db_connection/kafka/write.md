<a id="kafka-write"></a>

# Writing to Kafka

For writing data to Kafka, use `DBWriter` with specific options (see below).

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
: * `Avro.serialize_column`
  * `JSON.serialize_column`
  * `CSV.serialize_column`

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
