# Reading from Kafka { #kafka-read }

Data can be read from Kafka to Spark using [DBReader][db-reader].
It also supports  [strategy][strategy] for incremental data reading.

## Supported DBReader features

- ❌ `columns` (is not supported by Kafka)
- ❌ `where` (is not supported by Kafka)
- ✅︎ `hwm`, supported strategies:
- - ✅︎ [Snapshot strategy][snapshot-strategy]
- - ✅︎ [Incremental strategy][incremental-strategy]
- - ❌ [Snapshot batch strategy][snapshot-batch-strategy]
- - ❌ [Incremental batch strategy][incremental-batch-strategy]
- ❌ `hint` (is not supported by Kafka)
- ❌ `df_schema` (see note below)
- ✅︎ `options` (see [Kafka.ReadOptions][onetl.connection.db_connection.kafka.options.KafkaReadOptions])

## Dataframe schema

Unlike other DB connections, Kafka does not have concept of columns.
All the topics messages have the same set of fields, see structure below:

```text
root
|-- key: binary (nullable = true)
|-- value: binary (nullable = true)
|-- topic: string (nullable = false)
|-- partition: integer (nullable = false)
|-- offset: integer (nullable = false)
|-- timestamp: timestamp (nullable = false)
|-- timestampType: integer (nullable = false)
|-- headers: struct (nullable = true)
    |-- key: string (nullable = false)
    |-- value: binary (nullable = true)
```

`headers` field is present in the dataframe only if `Kafka.ReadOptions(include_headers=True)` is passed (compatibility with Kafka 1.x).

## Value deserialization

To read `value` or `key` of other type than bytes (e.g. struct or integer), users have to deserialize values manually.

This could be done using following methods:
  - [Avro.parse_column][onetl.file.format.avro.Avro.parse_column]
  - [JSON.parse_column][onetl.file.format.json.JSON.parse_column]
  - [CSV.parse_column][onetl.file.format.csv.CSV.parse_column]
  - [XML.parse_column][onetl.file.format.xml.XML.parse_column]

## Examples

Snapshot strategy, `value` is Avro binary data:

```python
from onetl.connection import Kafka
from onetl.db import DBReader, DBWriter
from onetl.file.format import Avro
from pyspark.sql.functions import decode

# read all topic data from Kafka
kafka = Kafka(...)
reader = DBReader(connection=kafka, source="avro_topic")
read_df = reader.run()

# parse Avro format to Spark struct
avro = Avro(
    schema_dict={
        "type": "record",
        "name": "Person",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"},
        ],
    }
)
deserialized_df = read_df.select(
    # cast binary key to string
    decode("key", "UTF-8").alias("key"),
    avro.parse_column("value"),
)
```

Incremental strategy, `value` is JSON string:

!!! note

    Currently Kafka connector does support only HWMs based on `offset` field. Other fields, like `timestamp`, are not yet supported.
```

```python
from onetl.connection import Kafka
from onetl.db import DBReader, DBWriter
from onetl.file.format import JSON
from pyspark.sql.functions import decode

kafka = Kafka(...)

# read only new data from Kafka topic
reader = DBReader(
    connection=kafka,
    source="topic_name",
    hwm=DBReader.AutoDetectHWM(name="kafka_hwm", expression="offset"),
)

with IncrementalStrategy():
    read_df = reader.run()

# parse JSON format to Spark struct
json = JSON()
schema = StructType(
    [
        StructField("name", StringType(), nullable=True),
        StructField("age", IntegerType(), nullable=True),
    ],
)
deserialized_df = read_df.select(
    # cast binary key to string
    decode("key", "UTF-8").alias("key"),
    json.parse_column("value", json),
)
```

## Options

<!-- 
```{eval-rst}
.. currentmodule:: onetl.connection.db_connection.kafka.options
```

```{eval-rst}
.. autopydantic_model:: KafkaReadOptions
    :member-order: bysource
    :model-show-field-summary: false
    :field-show-constraints: false
```
 -->

::: onetl.connection.db_connection.kafka.options.KafkaReadOptions
    options:
        inherited_members: true
        heading_level: 3
        show_root_heading: true