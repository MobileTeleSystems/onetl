import os
import re
from pathlib import Path
from textwrap import dedent

import pytest
from pydantic import ValidationError

from onetl.connection import Kafka

pytestmark = [pytest.mark.kafka, pytest.mark.db_connection, pytest.mark.connection]


@pytest.mark.parametrize(
    "spark_version,scala_version_input,scala_version_real",
    [
        ("2.3.0", None, "2.11"),
        ("2.3.0", "2.11", "2.11"),
        ("2.3.0", "2.12", "2.12"),
        ("3.3.0", None, "2.12"),
        ("3.3.0", "2.12", "2.12"),
        ("3.3.0", "2.13", "2.13"),
    ],
)
def test_kafka_jars(spark_version, scala_version_input, scala_version_real):
    # Assert
    assert Kafka.get_package_spark(
        spark_version=spark_version,
        scala_version=scala_version_input,
    ) == [f"org.apache.spark:spark-sql-kafka-0-10_{scala_version_real}:{spark_version}"]


@pytest.mark.parametrize(
    "arg, value",
    [
        ("assign", "assign_value"),
        ("subscribe", "subscribe_value"),
        ("subscribePattern", "subscribePattern_value"),
        ("startingOffsets", "startingOffsets_value"),
        ("startingOffsetsByTimestamp", "startingOffsetsByTimestamp_value"),
        ("startingTimestamp", "startingTimestamp_value"),
        ("endingOffsets", "endingOffsets_value"),
        ("endingOffsetsByTimestamp", "endingOffsetsByTimestamp_value"),
        ("startingOffsetsByTimestampStrategy", "startingOffsetsByTimestampStrategy_value"),
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
        ("security.protocol", "ssl"),
        ("sasl.mechanism*", "PLAIN"),
        ("kafka.sasl.jaas.config", "GSSAPI"),
        ("key.key_value", "key_value"),
        ("value.value", "value"),
    ],
)
def test_kafka_invalid_extras(spark_mock, arg, value):
    with pytest.raises(
        ValueError,
        match=re.escape("are not allowed to use in a Extra"),
    ):
        Kafka(spark=spark_mock, cluster="some_cluster", addresses=["192.168.1.1"], extra={arg: value})


@pytest.mark.parametrize(
    "arg, value",
    [
        ("kafka.group.id", "group_id"),
        ("group.id", "group_id"),
    ],
)
def test_kafka_valid_extras(spark_mock, arg, value):
    Kafka(spark=spark_mock, cluster="some_cluster", addresses=["192.168.1.1"], extra={arg: value})


def test_kafka_weak_permissons_keytab_error(spark_mock, create_keytab):
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


def test_kafka_wrong_path_keytab_error(spark_mock, tmp_path_factory):
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


def test_passed_only_keytab_error(spark_mock, create_keytab):
    # Assert
    msg = "principal\n  field required"
    with pytest.raises(
        ValueError,
        match=re.escape(msg),
    ):
        Kafka.KerberosAuth(
            keytab=create_keytab,
        )


def test_passed_only_pass_error(spark_mock):
    msg = "username\n  field required"
    with pytest.raises(
        ValueError,
        match=re.escape(msg),
    ):
        Kafka.BasicAuth(
            password="passwd",
        )


def test_passed_only_user_error(spark_mock):
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
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": dedent(
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
    print(conf)
    assert conf == {
        "kafka.sasl.mechanism": "GSSAPI",
        "kafka.sasl.jaas.config": dedent(
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
        "kafka.sasl.kerberos.service.name": "kafka",
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
        "kafka.sasl.mechanism": "GSSAPI",
        "kafka.sasl.jaas.config": dedent(
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
        "kafka.sasl.kerberos.service.name": "kafka",
    }

    Path("./keytab").unlink()
