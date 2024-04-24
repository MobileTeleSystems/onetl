.. _kafka-data-format-handling:

Data Format Handling
--------------------

Kafka topics can store data in various formats including ``JSON``, ``CSV``, ``Avro``, etc. Below are examples of how to handle data formats using custom methods for parsing and serialization integrated with Spark's DataFrame operations.

CSV Format Handling
-------------------

``DBReader``
~~~~~~~~~~~~

To handle CSV formatted data stored in Kafka topics, use the :obj:`CSV.parse_column <onetl.file.format.csv.CSV.parse_column>` method. This method allows you to convert a CSV string column directly into a structured Spark DataFrame using a specified schema.

.. code-block:: python

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType

    from onetl.db import DBReader
    from onetl.file.format import CSV
    from onetl.connection import Kafka

    spark = SparkSession.builder.appName("KafkaCSVExample").getOrCreate()

    kafka = Kafka(addresses=["kafka-broker1:9092"], cluster="example-cluster", spark=spark)
    csv = CSV(sep=",", encoding="utf-8")

    reader = DBReader(
        connection=kafka,
        topic="topic_name",
    )
    df = reader.run()

    df.show()
    # +----+--------+--------+---------+------+-----------------------+-------------+
    # |key |value   |topic   |partition|offset|timestamp              |timestampType|
    # +----+--------+--------+---------+------+-----------------------+-------------+
    # |[31]|Alice,20|topicCSV|0        |0     |2024-04-24 13:02:25.911|0            |
    # |[32]|Bob,25  |topicCSV|0        |1     |2024-04-24 13:02:25.922|0            |
    # +----+--------+--------+---------+------+-----------------------+-------------+

    # schema for parsing CSV data from Kafka
    csv_schema = StructType(
        [
            StructField("name", StringType(), nullable=True),
            StructField("age", IntegerType(), nullable=True),
        ]
    )

    parsed_df = df.select(csv.parse_column("value", csv_schema))
    parse_df.select("value").first()
    # Row(value=Row(name='Alice', age=20))

``DBWriter``
~~~~~~~~~~~~

To serialize structured data into CSV format and write it back to a Kafka topic, use the :obj:`CSV.serialize_column <onetl.file.format.csv.CSV.serialize_column>` method.

.. code-block:: python

    from onetl.db import DBWriter
    from onetl.file.format import CSV
    from onetl.connection import Kafka

    kafka = Kafka(...)
    csv = CSV(sep=",", encoding="utf-8")

    df.select("value").show()
    # +------------+
    # |value       |
    # +------------+
    # |{Alice, 20} |
    # |{Bob, 25}   |
    # +------------+


    # serializing data parsed in reading example into CSV format
    serialized_df = df.select(csv.serialize_column("value"))

    writer = DBWriter(connection=kafka, topic="topic_name")
    writer.run(serialized_df)


    serialized_df.show()
    # +---+-----------+
    # |key|value      |
    # +---+-----------+
    # |  1|"Alice,20" |
    # |  2|"Bob,25"   |
    # +---+-----------+

JSON Format Handling
--------------------

``DBReader``
~~~~~~~~~~~~

To process JSON formatted data from Kafka, use the :obj:`JSON.parse_column <onetl.file.format.json.JSON.parse_column>` method.

.. code-block:: python

    from onetl.file.format import JSON

    df.show()
    # +----+-------------------------+----------+---------+------+-----------------------+-------------+
    # |key |value                    |topic     |partition|offset|timestamp              |timestampType|
    # +----+-------------------------+----------+---------+------+-----------------------+-------------+
    # |[31]|{"name":"Alice","age":20}|topicKafka|0        |0     |2024-04-24 16:51:11.739|0            |
    # |[32]|{"name":"Bob","age":25}  |topicKafka|0        |1     |2024-04-24 16:51:11.749|0            |
    # +----+-------------------------+----------+---------+------+-----------------------+-------------+

    json = JSON()

    json_schema = StructType(
        [
            StructField("name", StringType(), nullable=True),
            StructField("age", IntegerType(), nullable=True),
        ]
    )

    parsed_json_df = df.select(json.parse_column("value", json_schema))

    parsed_json_df.first()
    # Row(value=Row(name='Alice', age=20))

``DBWriter``
~~~~~~~~~~~~

For serializing data into JSON format and sending it back to Kafka, use the :obj:`JSON.serialize_column <onetl.file.format.json.JSON.serialize_column>`.

.. code-block:: python

    from onetl.file.format import JSON

    df.show()
    # +-----------+
    # |value      |
    # +-----------+
    # |{Alice, 20}|
    # |{Bob, 25}  |
    # +-----------+

    json = JSON()

    serialized_json_df = df.select(json.serialize_column("data_column"))
    serialized_json_df.show()
    # +-------------------------+
    # |value                    |
    # +-------------------------+
    # |{"name":"Alice","age":20}|
    # |{"name":"Bob","age":25}  |
    # +-------------------------+
