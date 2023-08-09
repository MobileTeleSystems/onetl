import logging
import re
from pathlib import Path

import pytest

from onetl.connection import Kafka

pytestmark = [pytest.mark.kafka, pytest.mark.df_connection, pytest.mark.connection]


def test_kafka_check_plaintext_anonymous(spark, caplog):
    from tests.fixtures.processing.kafka import KafkaProcessing

    kafka_processing = KafkaProcessing()

    kafka = Kafka(
        addresses=[f"{kafka_processing.host}:{kafka_processing.port}"],
        cluster="cluster",
        spark=spark,
    )
    with caplog.at_level(logging.INFO):
        assert kafka.check() == kafka

    assert "type = Kafka" in caplog.text
    assert f"addresses = ['{kafka_processing.host}:{kafka_processing.port}']" in caplog.text
    assert "cluster = 'cluster'" in caplog.text
    assert "protocol = KafkaPlaintextProtocol()" in caplog.text
    assert "auth = None" in caplog.text
    assert "extra = {}" in caplog.text

    assert "Connection is available" in caplog.text


def test_kafka_check_plaintext_basic_auth(spark, caplog):
    from tests.fixtures.processing.kafka import KafkaProcessing

    kafka_processing = KafkaProcessing()

    kafka = Kafka(
        addresses=[f"{kafka_processing.host}:{kafka_processing.basic_auth_port}"],
        cluster="cluster",
        spark=spark,
        auth=Kafka.BasicAuth(
            username=kafka_processing.user,
            password=kafka_processing.password,
        ),
    )
    with caplog.at_level(logging.INFO):
        assert kafka.check() == kafka

    assert "type = Kafka" in caplog.text
    assert f"addresses = ['{kafka_processing.host}:{kafka_processing.basic_auth_port}']" in caplog.text
    assert "cluster = 'cluster'" in caplog.text
    assert "protocol = KafkaPlaintextProtocol()" in caplog.text
    assert f"auth = KafkaBasicAuth(user='{kafka_processing.user}', password=SecretStr('**********'))" in caplog.text
    assert "extra = {}" in caplog.text

    assert "Connection is available" in caplog.text


def test_kafka_check_error(spark):
    kafka = Kafka(
        addresses=["fake:9092"],
        cluster="cluster",
        spark=spark,
    )
    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        kafka.check()


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
