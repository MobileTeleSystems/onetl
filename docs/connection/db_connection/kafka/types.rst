.. _kafka-types:


Type cast
---------

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
    # |[31]|Alice,20|topic_n1|0        |0     |2024-04-24 13:02:25.911|0            |
    # |[32]|Bob,25  |topic_n1|0        |1     |2024-04-24 13:02:25.922|0            |
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
