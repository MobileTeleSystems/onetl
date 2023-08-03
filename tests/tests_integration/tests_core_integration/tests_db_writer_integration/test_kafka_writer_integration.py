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
    processing.assert_equal_df(df, other_frame=pd_df)
