import json
import secrets

import pytest

from onetl._util.spark import get_spark_version
from onetl.connection import Kafka
from onetl.db import DBReader

pytestmark = [pytest.mark.kafka, pytest.mark.db_connection, pytest.mark.connection]


@pytest.fixture(name="schema")
def dataframe_schema():
    from pyspark.sql.types import (
        FloatType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("id_int", LongType(), nullable=True),
            StructField("text_string", StringType(), nullable=True),
            StructField("hwm_int", LongType(), nullable=True),
            StructField("float_value", FloatType(), nullable=True),
        ],
    )


@pytest.fixture
def kafka_schema():
    from pyspark.sql.types import (
        BinaryType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("key", BinaryType(), nullable=True),
            StructField("value", BinaryType(), nullable=True),
            StructField("topic", StringType(), nullable=True),
            StructField("partition", IntegerType(), nullable=True),
            StructField("offset", LongType(), nullable=True),
            StructField("timestamp", TimestampType(), nullable=True),
            StructField("timestampType", IntegerType(), nullable=True),
        ],
    )
    return schema  # noqa:  WPS331


@pytest.fixture
def kafka_schema_with_headers():
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("key", BinaryType(), nullable=True),
            StructField("value", BinaryType(), nullable=True),
            StructField("topic", StringType(), nullable=True),
            StructField("partition", IntegerType(), nullable=True),
            StructField("offset", LongType(), nullable=True),
            StructField("timestamp", TimestampType(), nullable=True),
            StructField("timestampType", IntegerType(), nullable=True),
            StructField(
                "headers",
                ArrayType(
                    StructType(
                        [
                            StructField("key", StringType(), nullable=True),
                            StructField("value", BinaryType(), nullable=True),
                        ],
                    ),
                ),
                nullable=True,
            ),
        ],
    )
    return schema  # noqa:  WPS331


@pytest.fixture(name="kafka_processing")
def create_kafka_data(spark):
    from tests.fixtures.processing.kafka import KafkaProcessing

    topic = secrets.token_hex(5)
    proc = KafkaProcessing()
    df = proc.create_spark_df(spark)
    rows = [row.asDict() for row in df.collect()]

    for row_to_send in rows:
        proc.send_message(topic, json.dumps(row_to_send).encode("utf-8"))

    yield topic, proc, df
    # Release
    proc.delete_topic([topic])


def test_kafka_reader(spark, kafka_processing, schema):
    topic, processing, expected_df = kafka_processing

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    reader = DBReader(
        connection=kafka,
        source=topic,
    )
    df = reader.run()

    processing.assert_equal_df(processing.json_deserialize(df, df_schema=schema), other_frame=expected_df)


def test_kafka_reader_columns_and_types_without_headers(spark, kafka_processing, kafka_schema):
    topic, processing, _ = kafka_processing

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    reader = DBReader(
        connection=kafka,
        source=topic,
    )

    df = reader.run()

    assert df.schema == kafka_schema  # headers aren't included in schema if includeHeaders=False


def test_kafka_reader_columns_and_types_with_headers(spark, kafka_processing, kafka_schema_with_headers):
    if get_spark_version(spark).major < 3:
        pytest.skip("Spark 3.x or later is required to write/read 'headers' from Kafka messages")

    topic, processing, _ = kafka_processing

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    # Check that the DataFrame also has a "headers" column when includeHeaders=True
    reader = DBReader(
        connection=kafka,
        source=topic,
        options=Kafka.ReadOptions(includeHeaders=True),
    )

    df = reader.run()

    assert df.schema == kafka_schema_with_headers


def test_kafka_reader_topic_does_not_exist(spark, kafka_processing):
    _, processing, _ = kafka_processing

    kafka = Kafka(
        spark=spark,
        addresses=[f"{processing.host}:{processing.port}"],
        cluster="cluster",
    )

    reader = DBReader(
        connection=kafka,
        source="missing",
    )

    with pytest.raises(ValueError, match="Topic 'missing' doesn't exist"):
        reader.run()
