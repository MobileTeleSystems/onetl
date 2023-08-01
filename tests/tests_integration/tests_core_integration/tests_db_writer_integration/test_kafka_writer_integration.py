import secrets

import pytest

from onetl.connection import Kafka
from onetl.db import DBWriter

pytestmark = pytest.mark.kafka


@pytest.fixture(name="kafka_processing")
def create_kafka_df(spark):
    from tests.fixtures.processing.kafka import KafkaProcessing

    topic = secrets.token_hex(5)
    proc = KafkaProcessing()

    data = [(f"key{i}", f"value{i}") for i in range(1, 11)]
    df = spark.createDataFrame(data, ["key", "value"])

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

    data = processing.read_data_earliest(topic, num_messages=df.count(), timeout=3)
    read_df = spark.createDataFrame(data, ["key", "value"])
    assert len(data) == df.count()
    assert df.schema == read_df.schema
