import logging
import re
from pathlib import Path

import pytest

from onetl.connection import Kafka
from onetl.hooks import hook

pytestmark = [pytest.mark.kafka]


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


def test_kafka_db_connection_check_with_hooks(request, spark, caplog):
    from tests.fixtures.processing.kafka import KafkaProcessing

    kafka_processing = KafkaProcessing()

    @Kafka.Slots.normalize_cluster_name.bind
    @hook
    def normalize_cluster_name(cluster: str):
        return cluster.lower().replace("_", "-")

    @Kafka.Slots.get_known_clusters.bind
    @hook
    def get_known_clusters():
        return {"local"}

    @Kafka.Slots.normalize_address.bind
    @hook
    def normalize_address(address: str, cluster: str):
        if cluster == "local":
            return address.lower()

    @Kafka.Slots.get_cluster_addresses.bind
    @hook
    def get_cluster_addresses(cluster: str):
        if cluster == "local":
            return ["localhost:9094"]
        return None

    request.addfinalizer(normalize_cluster_name.disable)
    request.addfinalizer(get_known_clusters.disable)
    request.addfinalizer(normalize_address.disable)
    request.addfinalizer(get_cluster_addresses.disable)

    Kafka(
        cluster="local",
        spark=spark,
    ).check()

    with caplog.at_level(logging.DEBUG):
        Kafka(
            addresses=[f"{kafka_processing.host.upper()}:{kafka_processing.port}"],
            cluster="local",
            spark=spark,
        ).check()

        assert "Got ['localhost:9094']" in caplog.text

    with pytest.raises(
        ValueError,
        match=re.escape("Cluster 'kafka-cluster' is not in the known clusters list: ['local']"),
    ):
        Kafka(
            addresses=[f"{kafka_processing.host}:{kafka_processing.port}"],
            cluster="kafka-cluster",
            spark=spark,
        ).check()
