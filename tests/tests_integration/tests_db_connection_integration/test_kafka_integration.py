import re
from pathlib import Path

import pytest

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


from confluent_kafka import Producer


def delivery_report(err, msg):
    # Обработка сообщения после отправки
    if err is not None:
        print(f"Сообщение не отправлено: {err}")
    else:
        print(f"Сообщение успешно отправлено: {msg}")


def send_message(bootstrap_servers, topic, message):
    # Создание Kafka producer
    producer = Producer({"bootstrap.servers": bootstrap_servers})

    # Отправка сообщения
    producer.produce(topic, message.encode("utf-8"), callback=delivery_report)

    # Ожидание завершения отправки всех сообщений
    producer.flush()


def test_kafka_connection_read(spark):
    # Arrange
    bootstrap_servers = "onetl-githib-kafka-1:9092"
    topic = "test-topic"
    message = "Hello, Kafka 2!"

    send_message(bootstrap_servers, topic, message)
    # Act
    # Assert
