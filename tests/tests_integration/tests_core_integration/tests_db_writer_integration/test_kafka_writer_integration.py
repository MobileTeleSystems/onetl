import logging
import re
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
    processing.assert_equal_df(df, other_frame=pd_df.drop(columns=["key", "partition"], axis=1))
    # Check that the 'key' column is filled with nulls
    assert pd_df["key"].isnull().sum() == df.count()

    reader = DBReader(
        connection=kafka,
        source=topic,
        options=kafka.ReadOptions(includeHeaders=True),
    )
    df_read = reader.run()
    # Check that all values in 'headers' are null
    assert df_read.select("headers").na.drop().count() == 0


def test_kafka_writer_no_value_column_error(spark, kafka_processing):
    from pyspark.sql.utils import AnalysisException

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

    error_msg = "Invalid column names: {'invalid_column'}."
    with pytest.raises(ValueError, match=re.escape(error_msg)):
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
    data = [(row["value"], [("key", bytearray(b"value"))]) for row in df.collect()]
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
    processing.assert_equal_df(df, other_frame=pd_df.drop(columns=["partition"], axis=1))


def test_kafka_writer_topic_column(spark, kafka_processing, caplog):
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

    with caplog.at_level(logging.WARNING):
        writer.run(df)
        assert (
            f"The 'topic' column in the DataFrame will be overridden with the value in 'table' - '{topic}'"
            in caplog.text
        )

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

    pd_df = processing.get_expected_df(topic, num_messages=df.count(), timeout=3)

    # Check that the 'partition' column is filled with the default partition value - 0
    assert processing.get_num_partitions(topic) == 1
    assert pd_df["partition"].unique()[0] == 0


def test_kafka_writer_headers(spark, kafka_processing):
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

    reader = DBReader(
        connection=kafka,
        source=topic,
        options=kafka.ReadOptions(includeHeaders=True),
    )

    # Add a 'headers' column to the DataFrame
    headers_schema = ArrayType(
        StructType(
            [
                StructField("key", StringType()),
                StructField("value", BinaryType()),
            ],
        ),
    )
    data = [(row["value"], [("key", bytearray(b"value"))]) for row in df.collect()]
    df = spark.createDataFrame(data, schema=df.schema.add("headers", headers_schema))
    writer.run(df)

    df_read = reader.run()

    processing.assert_equal_df(
        df.select("headers"),
        other_frame=df_read.select("headers"),
    )
