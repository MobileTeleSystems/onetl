import logging

import pytest

from onetl.connection import Kafka

pytestmark = [pytest.mark.kafka, pytest.mark.db_connection, pytest.mark.connection]


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

    assert "|Kafka|" in caplog.text
    assert "addresses = [" in caplog.text
    assert f"'{kafka_processing.host}:{kafka_processing.port}'" in caplog.text
    assert "cluster = 'cluster'" in caplog.text
    assert "protocol = KafkaPlaintextProtocol()" in caplog.text
    assert "auth = None" in caplog.text
    assert "extra = {}" in caplog.text

    assert "Connection is available." in caplog.text


def test_kafka_check_plaintext_basic_auth(spark, caplog):
    from tests.fixtures.processing.kafka import KafkaProcessing

    kafka_processing = KafkaProcessing()

    kafka = Kafka(
        addresses=[f"{kafka_processing.host}:{kafka_processing.sasl_port}"],
        cluster="cluster",
        spark=spark,
        auth=Kafka.BasicAuth(
            username=kafka_processing.user,
            password=kafka_processing.password,
        ),
    )
    with caplog.at_level(logging.INFO):
        assert kafka.check() == kafka

    assert "|Kafka|" in caplog.text
    assert "addresses = [" in caplog.text
    assert f"'{kafka_processing.host}:{kafka_processing.sasl_port}'" in caplog.text
    assert "cluster = 'cluster'" in caplog.text
    assert "protocol = KafkaPlaintextProtocol()" in caplog.text
    assert f"auth = KafkaBasicAuth(user='{kafka_processing.user}', password=SecretStr('**********'))" in caplog.text
    assert "extra = {}" in caplog.text

    assert "Connection is available." in caplog.text


@pytest.mark.parametrize("digest", ["SHA-256", "SHA-512"])
def test_kafka_check_plaintext_scram_auth(digest, spark, caplog):
    from tests.fixtures.processing.kafka import KafkaProcessing

    kafka_processing = KafkaProcessing()

    kafka = Kafka(
        addresses=[f"{kafka_processing.host}:{kafka_processing.sasl_port}"],
        cluster="cluster",
        spark=spark,
        auth=Kafka.ScramAuth(
            username=kafka_processing.user,
            password=kafka_processing.password,
            digest=digest,
        ),
    )
    with caplog.at_level(logging.INFO):
        assert kafka.check() == kafka

    assert "|Kafka|" in caplog.text
    assert "addresses = [" in caplog.text
    assert f"'{kafka_processing.host}:{kafka_processing.sasl_port}'" in caplog.text
    assert "cluster = 'cluster'" in caplog.text
    assert "protocol = KafkaPlaintextProtocol()" in caplog.text
    assert (
        f"auth = KafkaScramAuth(user='{kafka_processing.user}', password=SecretStr('**********'), digest='{digest}')"
        in caplog.text
    )
    assert "extra = {}" in caplog.text

    assert "Connection is available." in caplog.text


def test_kafka_check_error(spark):
    kafka = Kafka(
        addresses=["fake:9092"],
        cluster="cluster",
        spark=spark,
    )
    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        kafka.check()
