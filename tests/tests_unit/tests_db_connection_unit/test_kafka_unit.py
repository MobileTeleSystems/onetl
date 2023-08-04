import os
import re
from pathlib import Path
from textwrap import dedent

import pytest

from onetl.connection import Kafka
from onetl.connection.db_connection.kafka.extra import KafkaExtra

pytestmark = [pytest.mark.kafka, pytest.mark.db_connection, pytest.mark.connection]


@pytest.mark.parametrize(
    "spark_version, scala_version, package",
    [
        ("2.4.0", None, "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0"),
        ("2.4.0", "2.11", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0"),
        ("2.4.0", "2.12", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0"),
        ("3.3.0", None, "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"),
        ("3.3.0", "2.12", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"),
        ("3.3.0", "2.13", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0"),
    ],
)
def test_kafka_get_packages(spark_version, scala_version, package):
    assert Kafka.get_packages(spark_version=spark_version, scala_version=scala_version) == [package]


@pytest.mark.parametrize(
    "spark_version",
    [
        "2.2.0",
        "2.3.1",
    ],
)
def test_kafka_get_packages_error(spark_version):
    with pytest.raises(ValueError, match=f"Spark version must be at least 2.4, got {spark_version}"):
        Kafka.get_packages(spark_version=spark_version)


def test_kafka_too_old_spark_error(spark_mock, mocker):
    msg = "Spark version must be at least 2.4, got 2.3.1"
    mocker.patch.object(spark_mock, "version", new="2.3.1")
    with pytest.raises(ValueError, match=msg):
        Kafka(
            cluster="some_cluster",
            addresses=["192.168.1.1"],
            spark=spark_mock,
        )


def test_kafka_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'org.apache.spark.sql.kafka010.KafkaSourceProvider'"
    with pytest.raises(ValueError, match=msg):
        Kafka(
            cluster="some_cluster",
            addresses=["192.168.1.1"],
            spark=spark_no_packages,
        )


@pytest.mark.parametrize(
    "arg, value",
    [
        ("assign", "assign_value"),
        ("subscribe", "subscribe_value"),
        ("subscribePattern", "subscribePattern_value"),
        ("startingOffsets", "startingOffsets_value"),
        ("startingOffsetsByTimestamp", "startingOffsetsByTimestamp_value"),
        ("startingTimestamp", "startingTimestamp_value"),
        ("endingTimestamp", "endingTimestamp_value"),
        ("endingOffsets", "endingOffsets_value"),
        ("endingOffsetsByTimestamp", "endingOffsetsByTimestamp_value"),
        (
            "startingOffsetsByTimestampStrategy",
            "startingOffsetsByTimestampStrategy_value",
        ),
        ("kafka.bootstrap.servers", "kafka.bootstrap.servers_value"),
        ("kafka.group.id", "kafka.group.id_value"),
        ("topic", "topic_value"),
    ],
)
def test_kafka_prohibited_options_error(arg, value):
    error_msg = rf"Options \['{arg}'\] are not allowed to use in a KafkaReadOptions"
    with pytest.raises(ValueError, match=error_msg):
        Kafka.ReadOptions(**{arg: value})
    error_msg = rf"Options \['{arg}'\] are not allowed to use in a KafkaWriteOptions"
    with pytest.raises(ValueError, match=error_msg):
        Kafka.WriteOptions(**{arg: value})


@pytest.mark.parametrize(
    "arg, value",
    [
        ("failOnDataLoss", "false"),
        ("kafkaConsumer.pollTimeoutMs", "30000"),
        ("fetchOffset.numRetries", "3"),
        ("fetchOffset.retryIntervalMs", "1000"),
        ("maxOffsetsPerTrigger", "1000"),
        ("minOffsetsPerTrigger", "500"),
        ("maxTriggerDelay", "2000"),
        ("minPartitions", "2"),
        ("groupIdPrefix", "testPrefix"),
        ("includeHeaders", "true"),
    ],
)
def test_kafka_allowed_read_options_no_error(arg, value):
    try:
        Kafka.ReadOptions(**{arg: value})
    except ValidationError:
        pytest.fail("ValidationError for ReadOptions raised unexpectedly!")


@pytest.mark.parametrize(
    "arg, value",
    [
        ("includeHeaders", "true"),
    ],
)
def test_kafka_allowed_write_options_no_error(arg, value):
    try:
        Kafka.WriteOptions(**{arg: value})
    except ValidationError:
        pytest.fail("ValidationError for Write options raised unexpectedly!")


def test_kafka_auth(spark_mock):
    # Act
    conn = Kafka(
        spark=spark_mock,
        cluster="some_cluster",
        addresses=["192.168.1.1"],
        auth=Kafka.BasicAuth(
            user="user",
            password="passwd",
        ),
    )

    # Assert
    assert conn.auth.user == "user"
    assert conn.cluster == "some_cluster"
    assert conn.auth.password != "passwd"
    assert conn.auth.password.get_secret_value() == "passwd"
    assert conn.addresses == ["192.168.1.1"]

    assert conn.instance_url == "kafka://some_cluster"


def test_kafka_anon_auth(spark_mock):
    # Act
    conn = Kafka(
        spark=spark_mock,
        cluster="some_cluster",
        addresses=["192.168.1.1"],
    )

    # Assert
    assert not conn.auth
    assert conn.cluster == "some_cluster"
    assert conn.addresses == ["192.168.1.1"]

    assert conn.instance_url == "kafka://some_cluster"


def test_kafka_auth_keytab(spark_mock, create_keytab):
    # Act
    conn = Kafka(
        spark=spark_mock,
        cluster="some_cluster",
        addresses=["192.168.1.1"],
        auth=Kafka.KerberosAuth(
            principal="user",
            keytab=create_keytab,
        ),
    )

    # Assert
    assert conn.auth.principal == "user"
    assert conn.cluster == "some_cluster"
    assert conn.addresses == ["192.168.1.1"]

    assert conn.instance_url == "kafka://some_cluster"


def test_kafka_auth_keytab_custom_param(spark_mock, create_keytab):
    # Act
    conn = Kafka(
        spark=spark_mock,
        cluster="some_cluster",
        addresses=["192.168.1.1"],
        auth=Kafka.KerberosAuth(
            principal="user",
            keytab=create_keytab,
            custom_parameter="some_parameter",
        ),
    )

    # Assert
    assert conn.auth.principal == "user"
    assert conn.auth.custom_parameter == "some_parameter"
    assert conn.cluster == "some_cluster"
    assert conn.addresses == ["192.168.1.1"]

    assert conn.instance_url == "kafka://some_cluster"


def test_kafka_empty_addresses(spark_mock):
    with pytest.raises(
        ValueError,
        match=re.escape("Passed empty parameter 'addresses'"),
    ):
        Kafka(
            spark=spark_mock,
            password="passwd",
            user="user",
            cluster="some_cluster",
            addresses=[],
        )


@pytest.mark.parametrize(
    "arg, value",
    [
        ("bootstrap.servers", "kafka.bootstrap.servers_value"),
        ("kafka.bootstrap.servers", "kafka.bootstrap.servers_value"),
        ("security.protocol", "ssl"),
        ("kafka.security.protocol", "ssl"),
        ("sasl.mechanism", "PLAIN"),
        ("kafka.sasl.mechanism", "PLAIN"),
        ("key.key_value", "key_value"),
        ("kafka.key.key_value", "key_value"),
        ("value.value", "value"),
        ("kafka.value.value", "value"),
    ],
)
def test_kafka_invalid_extras(arg, value):
    with pytest.raises(
        ValueError,
        match=re.escape("are not allowed to use in a KafkaExtra"),
    ):
        KafkaExtra.parse({arg: value})


@pytest.mark.parametrize(
    "arg, value",
    [
        ("kafka.group.id", "group_id"),
        ("group.id", "group_id"),
    ],
)
def test_kafka_valid_extras(arg, value):
    extra_dict = KafkaExtra.parse({arg: value}).dict()
    assert extra_dict["group.id"] == value


def test_kafka_weak_permissons_keytab_error(create_keytab):
    # Arrange
    os.chmod(create_keytab, 0o000)  # noqa: S103, WPS339

    # Assert
    msg = f"No access to file '{create_keytab}'"
    with pytest.raises(
        ValueError,
        match=re.escape(msg),
    ):
        Kafka.KerberosAuth(
            principal="user",
            keytab=create_keytab,
        )


def test_kafka_wrong_path_keytab_error():
    # Assert
    msg = "File 'some/path' is missing"
    with pytest.raises(
        ValueError,
        match=re.escape(msg),
    ):
        Kafka.KerberosAuth(
            principal="user",
            keytab="some/path",
        )


def test_passed_only_keytab_error(create_keytab):
    # Assert
    msg = "principal\n  field required"
    with pytest.raises(
        ValueError,
        match=re.escape(msg),
    ):
        Kafka.KerberosAuth(
            keytab=create_keytab,
        )


def test_passed_only_pass_error():
    msg = "username\n  field required"
    with pytest.raises(
        ValueError,
        match=re.escape(msg),
    ):
        Kafka.BasicAuth(
            password="passwd",
        )


def test_passed_only_user_error():
    msg = "password\n  field required"
    with pytest.raises(
        ValueError,
        match=re.escape(msg),
    ):
        Kafka.BasicAuth(
            user="user",
        )


def test_kafka_empty_cluster(spark_mock):
    with pytest.raises(
        ValueError,
        match=re.escape(
            "cluster\n  field required (type=value_error.missing)",
        ),
    ):
        Kafka(
            spark=spark_mock,
            addresses=["192.168.1.1"],
            auth=Kafka.BasicAuth(
                password="passwd",
                user="user",
            ),
        )


def test_kafka_connection_get_jaas_conf_password(spark_mock):
    # Arrange
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        auth=Kafka.BasicAuth(username="user", password="password"),
    )

    # Act
    conf = kafka.auth.get_options(kafka)
    # Assert
    assert conf == {
        "sasl.mechanism": "PLAIN",
        "sasl.jaas.config": dedent(
            """
            org.apache.kafka.common.security.plain.PlainLoginModule required
            username="user"
            password="password";""",
        ).strip(),
    }


def test_kafka_connection_get_jaas_conf_deploy_keytab_false(spark_mock, create_keytab):
    # Arrange
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        auth=Kafka.KerberosAuth(
            principal="user",
            keytab=create_keytab,
            deploy_keytab=False,
        ),
    )

    # Act
    conf = kafka.auth.get_options(kafka)
    # Assert
    assert conf == {
        "sasl.mechanism": "GSSAPI",
        "sasl.jaas.config": dedent(
            f"""
            com.sun.security.auth.module.Krb5LoginModule required
            useTicketCache=false
            principal="user"
            keyTab="{create_keytab}"
            serviceName="kafka"
            renewTicket=true
            storeKey=true
            useKeyTab=true
            debug=false;""",
        ).strip(),
        "sasl.kerberos.service.name": "kafka",
    }


def test_kafka_connection_get_jaas_conf_deploy_keytab_true(spark_mock, create_keytab):
    # Arrange
    # deploy_keytab=True by default
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        auth=Kafka.KerberosAuth(
            principal="user",
            keytab=create_keytab,
        ),
    )

    # Act
    conf = kafka.auth.get_options(kafka)
    # Assert
    assert conf == {
        "sasl.mechanism": "GSSAPI",
        "sasl.jaas.config": dedent(
            f"""
            com.sun.security.auth.module.Krb5LoginModule required
            useTicketCache=false
            principal="user"
            keyTab="{create_keytab.name}"
            serviceName="kafka"
            renewTicket=true
            storeKey=true
            useKeyTab=true
            debug=false;""",
        ).strip(),
        "sasl.kerberos.service.name": "kafka",
    }

    Path("./keytab").unlink()


def test_kafka_connection_protocol_with_auth(spark_mock, create_keytab):
    # Arrange
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        auth=Kafka.KerberosAuth(
            principal="user",
            keytab=create_keytab,
        ),
    )
    # Assert
    assert kafka.protocol.get_options(kafka) == {"security.protocol": "SASL_PLAINTEXT"}


def test_kafka_connection_protocol_without_auth(spark_mock, create_keytab):
    # Arrange
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
    )

    # Assert
    assert kafka.protocol.get_options(kafka) == {"security.protocol": "PLAINTEXT"}
