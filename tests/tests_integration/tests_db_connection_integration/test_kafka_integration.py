import json
import re
from pathlib import Path
import secrets
import pytest


from onetl.connection import Kafka
from tests.fixtures.processing.kafka import KafkaProcessing
from confluent_kafka.admin import AdminClient


@pytest.fixture(name="kafka_processing")
def create_kafka_data(spark):
    topic = secrets.token_hex(5)
    proc = KafkaProcessing()
    df = proc.create_spark_df(spark)
    rows = [row.asDict() for row in df.collect()]

    for row_to_send in rows:
        proc.send_message(topic, json.dumps(row_to_send).encode("utf-8"))

    yield topic, proc, df
    # Release
    admin = AdminClient({"bootstrap.servers": f"{proc.host}:{proc.port}"})
    # TODO: there is a problem with deleting it does not always delete the topic immediately, but after 3 seconds
    admin.delete_topics([topic])


def test_kafka_connection_get_jaas_conf_deploy_keytab_true(spark, create_keytab):
    # Arrange
    cloned_keytab = Path("./keytab")
    # deploy_keytab=True by default
    kafka = Kafka(
        spark=spark,
        addresses=["some_address"],
        cluster="cluster",
        auth=Kafka.KerberosAuth(
            principal="user",
            keytab=create_keytab,
        ),
    )

    assert not cloned_keytab.exists()

    # Act
    kafka.auth.get_options(kafka)

    # Assert
    assert cloned_keytab.exists()
    assert cloned_keytab.read_text() == create_keytab.read_text()
    cloned_keytab.unlink()


def test_kafka_connection_get_jaas_conf_deploy_keytab_true_error(spark):
    # Assert
    # deploy_keytab=True by default
    with pytest.raises(ValueError, match=re.escape("File '/not/a/keytab' is missing")):
        Kafka(
            spark=spark,
            addresses=["some_address"],
            cluster="cluster",
            auth=Kafka.KerberosAuth(
                principal="user",
                keytab="/not/a/keytab",
            ),
        )


def test_kafka_connection_read(spark, kafka_processing):
    # Arrange
    topic, proc, expected_df = kafka_processing
    # Act
    kafka = Kafka(
        spark=spark,
        addresses=[f"{proc.host}:{proc.port}"],
        cluster="cluster",
    )

    df = kafka.read_source_as_df(source=topic)
    # Assert

    proc.assert_equal_df(df, other_frame=expected_df)
