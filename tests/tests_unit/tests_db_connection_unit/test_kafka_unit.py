import os
import re

import pytest

from onetl.connection import Kafka

pytestmark = pytest.mark.kafka


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


def test_kafka_auth(spark_mock):
    # Act
    conn = Kafka(
        spark=spark_mock,
        password="passwd",
        user="user",
        cluster="some_cluster",
        addresses=["192.168.1.1"],
    )

    # Assert
    assert conn.user == "user"
    assert conn.cluster == "some_cluster"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
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
    assert not conn.user
    assert conn.cluster == "some_cluster"
    assert conn.addresses == ["192.168.1.1"]

    assert conn.instance_url == "kafka://some_cluster"


def test_kafka_auth_keytab(spark_mock, create_keytab):
    # Act
    conn = Kafka(
        spark=spark_mock,
        keytab=create_keytab,
        user="user",
        cluster="some_cluster",
        addresses=["192.168.1.1"],
    )

    # Assert
    assert conn.user == "user"
    assert conn.cluster == "some_cluster"
    assert conn.password != "passwd"
    assert conn.addresses == ["192.168.1.1"]

    assert conn.instance_url == "kafka://some_cluster"


def test_kafka_empty_addresses(spark_mock):
    with pytest.raises(ValueError, match=re.escape("Passed empty parameter 'addresses'")):
        Kafka(
            spark=spark_mock,
            password="passwd",
            user="user",
            cluster="some_cluster",
            addresses=[],
        )


def test_kafka_weak_permissons_keytab_error(spark_mock, create_keytab):
    # Arrange
    os.chmod(create_keytab, 0o000)  # noqa: S103, WPS339

    # Assert
    msg = (
        "Please provide either `keytab` and `user`, or `password` and "
        "`user` for Kerberos auth, or none of parameters for anonymous auth"
    )
    with pytest.raises(
        ValueError,
        match=re.escape(msg),
    ):
        Kafka(
            spark=spark_mock,
            keytab=create_keytab,
            user="user",
            cluster="some_cluster",
            addresses=["192.168.1.1"],
        )


def test_kafka_wrong_path_keytab_error(spark_mock, tmp_path_factory):
    # Assert
    msg = (
        "Please provide either `keytab` and `user`, or `password` and "
        "`user` for Kerberos auth, or none of parameters for anonymous auth"
    )
    with pytest.raises(
        ValueError,
        match=re.escape(msg),
    ):
        Kafka(
            spark=spark_mock,
            keytab="some/path",
            user="user",
            cluster="some_cluster",
            addresses=["192.168.1.1"],
        )


def test_kafka_passed_user_pass_keytab_error(spark_mock, create_keytab):
    # Assert
    msg = (
        "Please provide either `keytab` and `user`, or `password` and "
        "`user` for Kerberos auth, or none of parameters for anonymous auth"
    )
    with pytest.raises(
        ValueError,
        match=re.escape(msg),
    ):
        Kafka(
            spark=spark_mock,
            password="passwd",
            user="user",
            cluster="some_cluster",
            addresses=["192.168.1.1"],
            keytab=create_keytab,
        )


def test_passed_keytab_pass_error(spark_mock, create_keytab):
    # Assert
    msg = (
        "Please provide either `keytab` and `user`, or `password` and "
        "`user` for Kerberos auth, or none of parameters for anonymous auth"
    )
    with pytest.raises(
        ValueError,
        match=re.escape(msg),
    ):
        Kafka(
            spark=spark_mock,
            password="passwd",
            cluster="some_cluster",
            addresses=["192.168.1.1"],
            keytab=create_keytab,
        )


def test_passed_only_keytab_error(spark_mock, create_keytab):
    # Assert
    msg = (
        "Please provide either `keytab` and `user`, or `password` and "
        "`user` for Kerberos auth, or none of parameters for anonymous auth"
    )
    with pytest.raises(
        ValueError,
        match=re.escape(msg),
    ):
        Kafka(
            spark=spark_mock,
            cluster="some_cluster",
            addresses=["192.168.1.1"],
            keytab=create_keytab,
        )


def test_passed_only_pass_error(spark_mock):
    msg = (
        "Please provide either `keytab` and `user`, or `password` and "
        "`user` for Kerberos auth, or none of parameters for anonymous auth"
    )
    with pytest.raises(
        ValueError,
        match=re.escape(msg),
    ):
        Kafka(
            spark=spark_mock,
            password="passwd",
            cluster="some_cluster",
            addresses=["192.168.1.1"],
        )


def test_passed_only_user_errror(spark_mock):
    msg = (
        "Please provide either `keytab` and `user`, or `password` and "
        "`user` for Kerberos auth, or none of parameters for anonymous auth"
    )
    with pytest.raises(
        ValueError,
        match=re.escape(msg),
    ):
        Kafka(
            spark=spark_mock,
            user="user",
            cluster="some_cluster",
            addresses=["192.168.1.1"],
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
            password="passwd",
            user="user",
            addresses=["192.168.1.1"],
        )


def test_kafka_connection_get_jaas_conf_password(spark_mock):
    # Arrange
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        user="user",
        password="password",
    )

    # Act
    conf = kafka._get_jaas_conf()

    # Assert
    assert conf == (
        "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
        'serviceName="e21047b5be0df7652cd99feb4168e887"\n'
        'username="user"\n'
        'password="password";'
    )


def test_kafka_connection_get_jaas_conf_deploy_keytab_false(spark_mock, create_keytab):
    # Arrange
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        user="user",
        cluster="cluster",
        keytab=create_keytab,
        deploy_keytab=False,
    )

    # Act
    conf = kafka._get_jaas_conf()

    # Assert
    assert conf == (
        "com.sun.security.auth.module.Krb5LoginModule required\n"
        f'keyTab="{create_keytab}"\n'
        'principal="user"\n'
        'serviceName="e21047b5be0df7652cd99feb4168e887"\n'
        "renewTicket=true\n"
        "storeKey=true\n"
        "useKeyTab=true\n"
        "useTicketCache=false;"
    )


def test_kafka_connection_get_jaas_conf_deploy_keytab_true(spark_mock, create_keytab):
    # Arrange
    # deploy_keytab=True by default
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        user="user",
        cluster="cluster",
        keytab=create_keytab,
    )

    # Act
    conf = kafka._get_jaas_conf()

    # Assert
    assert conf == (
        "com.sun.security.auth.module.Krb5LoginModule required\n"
        f'keyTab="{create_keytab.name}"\n'
        'principal="user"\n'
        'serviceName="e21047b5be0df7652cd99feb4168e887"\n'
        "renewTicket=true\n"
        "storeKey=true\n"
        "useKeyTab=true\n"
        "useTicketCache=false;"
    )
