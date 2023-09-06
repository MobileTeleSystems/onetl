import contextlib
import logging
import re
import secrets

import pytest

from onetl._util.spark import get_spark_version
from onetl.connection import Kafka
from onetl.db import DBWriter
from onetl.exception import TargetAlreadyExistsError

pytestmark = pytest.mark.kafka


@pytest.fixture(name="kafka_processing")
def create_kafka_processing(spark):
    from tests.fixtures.processing.kafka import KafkaProcessing

    topic = secrets.token_hex(5)
    proc = KafkaProcessing()

    yield topic, proc

    proc.delete_topic([topic])


@pytest.fixture
def kafka_spark_df(spark, kafka_processing):
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    topic, processing = kafka_processing
    df = processing.create_spark_df(spark)
    df = processing.json_serialize(df)

    headers_schema = ArrayType(
        StructType(
            [
                StructField("key", StringType()),
                StructField("value", BinaryType()),
            ],
        ),
    )
    partition_schema = IntegerType()
    topic_schema = StringType()
    key_schema = StringType()

    schema = df.schema
    schema = schema.add("key", key_schema)
    schema = schema.add("headers", headers_schema)
    schema = schema.add("partition", partition_schema)
    schema = schema.add("topic", topic_schema)
    data = [(row["value"], "key", [("key", bytearray(b"value"))], 0, topic) for row in df.collect()]

    return spark.createDataFrame(data, schema=schema)


def test_kafka_writer_snapshot(spark, kafka_processing, kafka_spark_df):
    from pyspark.sql.functions import lit

    if get_spark_version(spark).major < 3:
        pytest.skip("Spark 3.x or later is required to write/read 'headers' from Kafka messages")

    topic, processing = kafka_processing
    df = kafka_spark_df.select("value")

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

    pd_df = processing.get_expected_df(topic, num_messages=df.count())

    read_df = (
        df.withColumn("key", lit(None))
        .withColumn("topic", lit(topic))
        .withColumn("partition", lit(0))
        .withColumn("headers", lit(None))
    )
    processing.assert_equal_df(pd_df, other_frame=read_df)


def test_kafka_writer_no_value_column_error(spark, kafka_processing, kafka_spark_df):
    from pyspark.sql.utils import AnalysisException

    topic, processing = kafka_processing
    df = kafka_spark_df.select("key")

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


@pytest.mark.parametrize(
    "column, value",
    [
        ("offset", 0),
        ("timestamp", 10000),
        ("timestampType", 1),
        ("unknown", "str"),
    ],
)
def test_kafka_writer_invalid_column_error(
    column,
    value,
    spark,
    kafka_processing,
    kafka_spark_df,
):
    from pyspark.sql.functions import lit

    topic, processing = kafka_processing

    # Add an unexpected column to the DataFrame
    df = kafka_spark_df.withColumn(column, lit(value))

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    writer = DBWriter(
        connection=kafka,
        table=topic,
    )

    error_msg = (
        f"Invalid column names: ['{column}']. "
        "Expected columns: ['value'] (required), ['headers', 'key', 'partition'] (optional)"
    )
    with pytest.raises(ValueError, match=re.escape(error_msg)):
        writer.run(df)


def test_kafka_writer_key_column(spark, kafka_processing, kafka_spark_df):
    topic, processing = kafka_processing
    df = kafka_spark_df.select("value", "key")

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

    pd_df = processing.get_expected_df(topic, num_messages=df.count())
    assert len(pd_df) == df.count()
    processing.assert_equal_df(df, other_frame=pd_df.drop(columns=["partition", "headers", "topic"], axis=1))


def test_kafka_writer_topic_column(spark, kafka_processing, caplog, kafka_spark_df):
    from pyspark.sql.functions import lit

    topic, processing = kafka_processing
    original_df = kafka_spark_df.select("value")

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    writer = DBWriter(
        connection=kafka,
        table=topic,
    )
    writer.run(original_df)
    assert processing.topic_exists(topic)

    df = original_df.withColumn("topic", lit("other_topic"))
    with caplog.at_level(logging.WARNING):
        writer.run(df)
        assert f"The 'topic' column in the DataFrame will be overridden with value '{topic}'" in caplog.text

    assert not processing.topic_exists("other_topic")


def test_kafka_writer_partition_column(spark, kafka_processing, kafka_spark_df):
    topic, processing = kafka_processing
    df = kafka_spark_df.select("value", "partition")

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

    pd_df = processing.get_expected_df(topic, num_messages=df.count())

    # Check that the 'partition' column is filled with the default partition value - 0
    assert processing.get_num_partitions(topic) == 1
    assert (pd_df["partition"] == 0).all()


def test_kafka_writer_headers(spark, kafka_processing, kafka_spark_df):
    if get_spark_version(spark).major < 3:
        msg = f"kafka.WriteOptions(include_headers=True) requires Spark 3.x, got {spark.version}"
        context_manager = pytest.raises(ValueError, match=re.escape(msg))
    else:
        context_manager = contextlib.nullcontext()

    topic, processing = kafka_processing

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    writer = DBWriter(
        connection=kafka,
        table=topic,
        options=kafka.WriteOptions(include_headers=True),
    )

    df = kafka_spark_df.select("value", "headers")
    with context_manager:
        writer.run(df)

        pd_df = processing.get_expected_df(topic, num_messages=kafka_spark_df.count())

        processing.assert_equal_df(
            df,
            other_frame=pd_df.drop(columns=["key", "partition", "topic"], axis=1),
        )


def test_kafka_writer_headers_without_include_headers_fail(spark, kafka_processing, kafka_spark_df):
    topic, processing = kafka_processing

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    writer = DBWriter(
        connection=kafka,
        table=topic,
        options=kafka.WriteOptions(include_headers=False),
    )

    with pytest.raises(ValueError, match="Cannot write 'headers' column"):
        writer.run(kafka_spark_df)


def test_kafka_writer_mode(spark, kafka_processing, kafka_spark_df):
    from pyspark.sql.functions import lit

    topic, processing = kafka_processing
    df = kafka_spark_df.select("value")

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
    writer.run(df)

    pd_df = processing.get_expected_df(topic, num_messages=2 * kafka_spark_df.count())
    read_df = df.withColumn("key", lit(None)).withColumn("topic", lit(topic)).withColumn("partition", lit(0))

    # Check that second dataframe record is appended to first dataframe in same topic
    processing.assert_equal_df(pd_df.drop(columns=["headers"], axis=1), other_frame=read_df.union(read_df))


def test_kafka_writer_mode_error(spark, kafka_processing, kafka_spark_df):
    topic, processing = kafka_processing
    df = kafka_spark_df.select("value")
    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    writer = DBWriter(
        connection=kafka,
        table=topic,
        options=kafka.WriteOptions(if_exists="error"),
    )

    # Write is successful as topic didn't exist
    writer.run(df)

    with pytest.raises(TargetAlreadyExistsError, match=f"Topic {topic} already exists"):
        writer.run(df)
