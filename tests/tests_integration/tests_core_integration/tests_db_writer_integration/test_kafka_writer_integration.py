import secrets

import pytest

from onetl.connection import Kafka
from onetl.db import DBWriter

pytestmark = pytest.mark.kafka


@pytest.fixture(name="df")
def create_kafka_df(spark):
    from tests.fixtures.processing.kafka import KafkaProcessing

    topic = secrets.token_hex(5)
    proc = KafkaProcessing()

    data = [(f"key{i}", f"value{i}") for i in range(1, 11)]
    df = spark.createDataFrame(data, ["key", "value"])

    yield topic, proc, df

    proc.delete_topic([topic])


def test_kafka_writer_snapshot(spark, df):
    topic, processing, df = df
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

    messages = processing.read_data_earliest(topic)
    assert len(messages) == df.count()

    data = [(k.decode("utf-8"), v.decode("utf-8")) for k, v in messages]
    read_df = spark.createDataFrame(data, ["key", "value"])
    assert df.schema == read_df.schema
