# Reading from Kafka { #DBR-onetl-connection-db-connection-kafka-read-reading-from-kafka }

Data can be read from Kafka to Spark using [DBReader][DBR-onetl-db-reader].
It also supports [strategy][DBR-onetl-strategy-read-strategies] for incremental data reading.

## Supported DBReader features { #DBR-onetl-connection-db-connection-kafka-read-supported-dbreader-features }

- ❌ `columns` (is not supported by Kafka)
- ❌ `where` (is not supported by Kafka)
- ✅︎ `hwm`, supported strategies:
    - ✅︎ [Snapshot strategy][DBR-onetl-strategy-snapshot-strategy]
    - ✅︎ [Incremental strategy][DBR-onetl-connection-db-connection-clickhouse-read-incremental-strategy]
    - ❌ [Snapshot batch strategy][DBR-onetl-strategy-snapshot-batch-strategy]
    - ❌ [Incremental batch strategy][DBR-onetl-strategy-incremental-batch-strategy]
- ❌ `hint` (is not supported by Kafka)
- ❌ `df_schema` (see note below)
- ✅︎ `options` (see [Kafka.ReadOptions][onetl.connection.db_connection.kafka.options.KafkaReadOptions])

## Dataframe schema { #DBR-onetl-connection-db-connection-kafka-read-dataframe-schema }

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

## Value deserialization { #DBR-onetl-connection-db-connection-kafka-read-value-deserialization }

To read `value` or `key` of other type than bytes (e.g. struct or integer), users have to deserialize values manually.

This could be done using following methods:

- [Avro.parse_column][onetl.file.format.avro.Avro.parse_column]
- [JSON.parse_column][onetl.file.format.json.JSON.parse_column]
- [CSV.parse_column][onetl.file.format.csv.CSV.parse_column]
- [XML.parse_column][onetl.file.format.xml.XML.parse_column]

Or any other method provided by Spark or third-larty libraries which can parse `BinaryType()` column into useful data.

## GroupIds and offsets { #DBR-onetl-connection-db-connection-kafka-read-groupids-and-offsets }

Regular Kafka consumers use `subscrube(topic)` method to notify Kafka that some new data from Kafka should be send to consumer if available. Offsets read by group are committed to Kafka, to guarantee at-least-once even if consumer failed somethere.

Spark connector for Kafka is very different. It uses `assign(topic)` to read data manually from a topic. It doesn't commit offsets to Kafka, as the same data can be read multiple times, e.g. task failed and lost all its memory, new task will read this data again.

## Examples { #DBR-onetl-connection-db-connection-kafka-read-examples }

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

## Options { #DBR-onetl-connection-db-connection-kafka-read-options }


::: onetl.connection.db_connection.kafka.options.KafkaReadOptions
    options:
        inherited_members: true
