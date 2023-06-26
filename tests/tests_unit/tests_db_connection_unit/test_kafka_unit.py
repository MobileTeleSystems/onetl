import os
import re

import pytest

from onetl.connection import Kafka

pytestmark = pytest.mark.kafka


@pytest.mark.parametrize(
    "scala_version,spark_version",
    [
        ("2.11", "2.3.0"),
        ("2.11", "2.3.1"),
        ("2.11", "2.3.2"),
        ("2.11", "2.3.3"),
        ("2.11", "2.3.4"),
        ("2.11", "2.4.0"),
        ("2.11", "2.4.1"),
        ("2.11", "2.4.2"),
        ("2.11", "2.4.3"),
        ("2.11", "2.4.4"),
        ("2.11", "2.4.5"),
        ("2.11", "2.4.6"),
        ("2.11", "2.4.7"),
        ("2.12", "3.0.0"),
        ("2.12", "3.0.1"),
        ("2.12", "3.0.2"),
        ("2.12", "3.0.3"),
        ("2.12", "3.1.0"),
        ("2.12", "3.1.1"),
        ("2.12", "3.1.2"),
        ("2.12", "3.0.0"),
        ("2.12", "3.1.3"),
        ("2.12", "3.2.0"),
        ("2.12", "3.2.1"),
        ("2.12", "3.2.2"),
        ("2.12", "3.2.3"),
        ("2.12", "3.2.4"),
        ("2.12", "3.3.0"),
        ("2.12", "3.3.1"),
        ("2.12", "3.3.2"),
        ("2.12", "3.4.0"),
    ],
)
def test_kafka_jars(spark_version, scala_version):
    assert (
        Kafka.get_package_spark(
            spark_version=spark_version,
            scala_version=scala_version,
        )
        == f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"
    )


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


def test_kafka_auth_keytab(spark_mock, load_keytab):
    # Act
    conn = Kafka(
        spark=spark_mock,
        keytab=load_keytab,
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


def test_kafka_weak_permissons_keytab_error(spark_mock, load_keytab):
    # Arrange
    os.chmod(load_keytab, 0o000)  # noqa: S103, WPS339

    # Assert
    with pytest.raises(
        ValueError,
        match=re.escape("Keytab file permission denied. File properties:"),
    ):
        Kafka(
            spark=spark_mock,
            keytab=load_keytab,
            user="user",
            cluster="some_cluster",
            addresses=["192.168.1.1"],
        )


def test_kafka_wrong_path_keytab_error(spark_mock, tmp_path_factory):
    # Assert
    with pytest.raises(
        ValueError,
        match=re.escape("The file does not exists. File  properties:  "),
    ):
        Kafka(
            spark=spark_mock,
            keytab="some/path",
            user="user",
            cluster="some_cluster",
            addresses=["192.168.1.1"],
        )


def test_kafka_passed_user_pass_keytab_error(spark_mock, load_keytab):
    # Assert
    msg = (
        "If you passed the `user` parameter please provide either `keytab` or `password` for auth, "
        "not both. Or do not specify `user`, "
        "`keytab` and `password` parameters for anonymous authorization."
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
            keytab=load_keytab,
        )


def test_passed_keytab_pass_error(spark_mock, load_keytab):
    # Assert
    with pytest.raises(
        ValueError,
        match=re.escape(
            "`user` parameter not passed. Passed `password` and `keytab` parameters. Passing either `password` or "
            "`keytab` is allowed.",
        ),
    ):
        Kafka(
            spark=spark_mock,
            password="passwd",
            cluster="some_cluster",
            addresses=["192.168.1.1"],
            keytab=load_keytab,
        )


def test_passed_only_keytab_error(spark_mock, load_keytab):
    # Assert
    with pytest.raises(
        ValueError,
        match=re.escape("Passed `keytab` without `user` parameter."),
    ):
        Kafka(
            spark=spark_mock,
            cluster="some_cluster",
            addresses=["192.168.1.1"],
            keytab=load_keytab,
        )


def test_passed_only_pass_error(spark_mock):
    with pytest.raises(
        ValueError,
        match=re.escape("Passed `password` without `user` parameter."),
    ):
        Kafka(
            spark=spark_mock,
            password="passwd",
            cluster="some_cluster",
            addresses=["192.168.1.1"],
        )


def test_passed_only_user_errror(spark_mock):
    with pytest.raises(
        ValueError,
        match=re.escape("Passed only `user` parameter without `password` or `keytab`."),
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
