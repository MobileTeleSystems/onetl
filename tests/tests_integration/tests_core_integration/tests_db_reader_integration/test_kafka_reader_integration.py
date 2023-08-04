import json
import secrets

import pytest

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
            StructField("id_int", LongType(), True),
            StructField("text_string", StringType(), True),
            StructField("hwm_int", LongType(), True),
            StructField("float_value", FloatType(), True),
        ],
    )


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
    # Arrange
    topic, processing, expected_df = kafka_processing

    # Act
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

    # Assert
    processing.assert_equal_df(processing.json_deserialize(df, df_schema=schema), other_frame=expected_df)


def test_kafka_reader_columns_and_types(spark, kafka_processing):
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

    expected_schema = kafka.get_df_schema(topic)
    assert expected_schema[:-1] == df.schema  # headers aren't included in schema if includeHeaders=False

    # Check that the DataFrame also has a "headers" column when includeHeaders=True
    reader = DBReader(
        connection=kafka,
        source=topic,
        options=kafka.ReadOptions(includeHeaders=True),
    )

    df = reader.run()

    expected_schema = kafka.get_df_schema(topic)
    assert df.schema == expected_schema
