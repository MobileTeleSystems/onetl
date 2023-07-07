from pathlib import Path

from onetl.connection import Kafka


def test_kafka_connection_get_jaas_conf_deploy_keytab_true(spark, create_keytab):
    # Arrange
    # deploy_keytab=True by default
    kafka = Kafka(
        spark=spark,
        addresses=["some_address"],
        user="user",
        cluster="cluster",
        keytab=create_keytab,
    )

    # Act
    kafka._get_jaas_conf()

    # Assert
    cloned_keytab = Path("./keytab")
    assert cloned_keytab.exists()
    assert cloned_keytab.read_text() == create_keytab.read_text()
    cloned_keytab.unlink()
