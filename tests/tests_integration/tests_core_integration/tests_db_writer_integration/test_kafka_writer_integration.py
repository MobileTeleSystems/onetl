import secrets

import pytest

from onetl.connection import Kafka
from onetl.db import DBReader, DBWriter

pytestmark = pytest.mark.kafka


@pytest.fixture(name="kafka_processing")
def create_kafka_df(spark):
    from tests.fixtures.processing.kafka import KafkaProcessing

    topic = secrets.token_hex(5)
    proc = KafkaProcessing()

    df = proc.create_spark_df(spark)
    df = proc.json_serialize(df)

    yield topic, proc, df

    proc.delete_topic([topic])


def test_kafka_writer_snapshot(spark, kafka_processing):
    topic, processing, df = kafka_processing
    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    writer = DBWriter(
        connection=kafka,
        table=topic,
    )
    writer.run(df)

    pd_df = processing.get_expected_df(topic, num_messages=df.count(), timeout=3)
    assert len(pd_df) == df.count()
    processing.assert_equal_df(df, other_frame=pd_df.drop("key", axis=1))


def test_kafka_writer_no_value_column_error(spark, kafka_processing):
    from pyspark.errors.exceptions.captured import AnalysisException

    topic, processing, df = kafka_processing

    df = df.drop("value")

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    writer = DBWriter(
        connection=kafka,
        table=topic,
    )

    with pytest.raises(AnalysisException, match="Required attribute 'value' not found"):
        writer.run(df)


def test_kafka_writer_invalid_column_error(spark, kafka_processing):
    from pyspark.sql.functions import lit

    topic, processing, df = kafka_processing

    # Add an unexpected column to the DataFrame
    df = df.withColumn("invalid_column", lit("invalid_value"))

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    writer = DBWriter(
        connection=kafka,
        table=topic,
    )

    with pytest.raises(ValueError, match="Invalid column names"):
        writer.run(df)


def test_kafka_writer_with_include_headers_error(spark, kafka_processing):
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        StringType,
        StructField,
        StructType,
    )

    topic, processing, df = kafka_processing

    # Add a 'headers' column to the DataFrame
    headers_schema = ArrayType(
        StructType(
            [
                StructField("key", StringType()),
                StructField("value", BinaryType()),
            ],
        ),
    )
    data = [(row["value"], [("key", b"value")]) for row in df.collect()]
    df = spark.createDataFrame(data, schema=df.schema.add("headers", headers_schema))

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    writer = DBWriter(
        connection=kafka,
        table=topic,
        options=kafka.WriteOptions(includeHeaders=False),
    )

    with pytest.raises(ValueError, match="Cannot write 'headers' column"):
        writer.run(df)


def test_kafka_writer_key_column(spark, kafka_processing):
    from pyspark.sql.functions import lit

    topic, processing, df = kafka_processing

    df = df.withColumn("key", lit("key_value"))

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    writer = DBWriter(
        connection=kafka,
        table=topic,
    )
    writer.run(df)

    pd_df = processing.get_expected_df(topic, num_messages=df.count(), timeout=3)
    assert len(pd_df) == df.count()
    processing.assert_equal_df(df, other_frame=pd_df)

    # Remove the 'key' column from the DataFrame
    df_no_key = df.drop("key")
    writer.run(df_no_key)
    # Check that the 'key' column is filled with nulls
    pd_df = processing.get_expected_df(topic, num_messages=df.count() + df_no_key.count(), timeout=3)
    assert pd_df["key"].isnull().sum() == df_no_key.count()


def test_kafka_writer_topic_column(spark, kafka_processing):
    from pyspark.sql.functions import lit

    topic, processing, df = kafka_processing

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    writer = DBWriter(
        connection=kafka,
        table=topic,
    )
    writer.run(df)

    assert processing.topic_exists(topic)

    df = df.withColumn("topic", lit("other_topic"))
    writer.run(df)

    # Check that the 'topic' matches the table name (not the "other_topic")
    assert processing.topic_exists(topic)
    assert not processing.topic_exists("other_topic")


def test_kafka_writer_partition_column(spark, kafka_processing):
    topic, processing, df = kafka_processing

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    writer = DBWriter(
        connection=kafka,
        table=topic,
    )
    writer.run(df)

    # Read the data from Kafka
    reader = DBReader(
        connection=kafka,
        source=topic,
    )
    df_read = reader.run()

    # Check that the 'partition' column is filled with the default partition value
    assert df_read.select("partition").distinct().collect()[0][0] == 0
    assert processing.get_num_partitions(topic) == 1


def test_kafka_writer_headers(spark, kafka_processing):
    from pyspark.sql import functions as F  # noqa: N812
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        StringType,
        StructField,
        StructType,
    )

    topic, processing, df = kafka_processing

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    writer = DBWriter(
        connection=kafka,
        table=topic,
        options=kafka.WriteOptions(includeHeaders=True),
    )

    writer.run(df)

    # Read the data from Kafka
    reader = DBReader(
        connection=kafka,
        source=topic,
        options=kafka.ReadOptions(includeHeaders=True),
    )
    df_read = reader.run()

    assert df_read.filter(F.col("headers").isNull()).count() == df.count()

    # Add a 'headers' column to the DataFrame
    headers_schema = ArrayType(
        StructType(
            [
                StructField("key", StringType()),
                StructField("value", BinaryType()),
            ],
        ),
    )
    data = [(row["value"], [("key", b"value")]) for row in df.collect()]
    df = spark.createDataFrame(data, schema=df.schema.add("headers", headers_schema))
    writer.run(df)

    df_read = reader.run()

    assert df_read.filter(F.col("headers").isNotNull()).count() == df.count()
    processing.assert_equal_df(
        df.select("headers"),
        other_frame=df_read.filter(F.col("headers").isNotNull()).select("headers"),
    )
