from datetime import datetime

import re
from confluent_kafka import Producer
from pathlib import Path
import secrets
import pytest
from pyspark.sql.types import (
    StructField,
    BinaryType,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    StructType,
    Row,
)

from onetl.connection import Kafka


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


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


def send_message(bootstrap_servers, topic, message):
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    producer.produce(topic, message.encode("utf-8"), callback=delivery_report)
    producer.flush()


def test_kafka_connection_read(spark):
    # Arrange
    bootstrap_servers = "onetl-githib-kafka-1:9092"
    topic = secrets.token_hex(5)
    message = "test"

    send_message(bootstrap_servers, topic, message)

    # Act
    kafka = Kafka(
        spark=spark,
        addresses=[bootstrap_servers],
        cluster="cluster",
    )

    df = kafka.read_source_as_df(source=topic)
    # Assert
    assert df.schema == StructType(
        [
            StructField("key", BinaryType(), True),
            StructField("value", BinaryType(), True),
            StructField("topic", StringType(), True),
            StructField("partition", IntegerType(), True),
            StructField("offset", LongType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("timestampType", IntegerType(), True),
        ],
    )

    df = df.collect()
    write_time = df[0][5]

    assert df == [
        Row(
            key=None,
            value=bytearray(b"test"),
            topic=topic,
            partition=0,
            offset=0,
            timestamp=write_time,
            timestampType=0,
        ),
    ]

    # Release
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    admin.delete_topics([topic])
