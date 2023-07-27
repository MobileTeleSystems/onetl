import json
import secrets
from time import sleep

import pytest
from confluent_kafka.admin import AdminClient

from onetl.connection import Kafka
from onetl.db import DBReader


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
    admin = AdminClient({"bootstrap.servers": f"{proc.host}:{proc.port}"})
    admin.delete_topics([topic])
    sleep(3)


def test_kafka_reader(spark, kafka_processing):
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
    processing.assert_equal_df(df, other_frame=expected_df)
