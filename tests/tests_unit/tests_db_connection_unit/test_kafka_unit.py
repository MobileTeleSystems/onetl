import os
import re
from pathlib import Path

import pytest

from onetl.connection import Kafka

pytestmark = pytest.mark.kafka


def test_kafka_class_attributes():
    Kafka.package_spark_2_0_2 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.0.2"
    Kafka.package_spark_2_1_0 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0"
    Kafka.package_spark_2_1_1 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1"
    Kafka.package_spark_2_1_2 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.2"
    Kafka.package_spark_2_1_3 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.3"
    Kafka.package_spark_2_2_0 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0"
    Kafka.package_spark_2_2_1 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1"
    Kafka.package_spark_2_2_2 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.2"
    Kafka.package_spark_2_2_3 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.3"
    Kafka.package_spark_2_3_0 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0"
    Kafka.package_spark_2_3_1 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1"
    Kafka.package_spark_2_3_2 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2"
    Kafka.package_spark_2_3_3 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.3"
    Kafka.package_spark_2_3_4 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4"
    Kafka.package_spark_2_4_0 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0"
    Kafka.package_spark_2_4_1 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1"
    Kafka.package_spark_2_4_2 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.2"
    Kafka.package_spark_2_4_3 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3"
    Kafka.package_spark_2_4_4 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4"
    Kafka.package_spark_2_4_5 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5"
    Kafka.package_spark_2_4_6 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6"
    Kafka.package_spark_2_4_7 = "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7"
    Kafka.package_spark_3_0_0 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0"
    Kafka.package_spark_3_0_1 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"
    Kafka.package_spark_3_0_2 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.2"
    Kafka.package_spark_3_0_3 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.3"
    Kafka.package_spark_3_1_0 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.0"
    Kafka.package_spark_3_1_1 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1"
    Kafka.package_spark_3_1_2 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2"
    Kafka.package_spark_3_1_3 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3"
    Kafka.package_spark_3_2_0 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0"
    Kafka.package_spark_3_2_1 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1"
    Kafka.package_spark_3_2_2 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2"
    Kafka.package_spark_3_2_3 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3"
    Kafka.package_spark_3_2_4 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4"
    Kafka.package_spark_3_3_0 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
    Kafka.package_spark_3_3_1 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1"
    Kafka.package_spark_3_3_2 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2"
    Kafka.package_spark_3_4_0 = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"


def test_kafka_auth(spark_mock):
    # Act
    conn = Kafka(spark=spark_mock, password="passwd", user="user", cluster="some_cluster", addresses=["192.168.1.1"])

    # Assert
    assert conn.user == "user"
    assert conn.cluster == "some_cluster"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.addresses == ["192.168.1.1"]

    assert conn.instance_url == "kafka://some_cluster"


def test_kafka_auth_keytab(spark_mock, tmp_path_factory):
    # Arrange
    path = Path(tmp_path_factory.mktemp("data") / "keytab")
    path.write_text("content")

    # Act
    conn = Kafka(spark=spark_mock, keytab=path, user="user", cluster="some_cluster", addresses=["192.168.1.1"])

    # Assert
    assert conn.user == "user"
    assert conn.cluster == "some_cluster"
    assert conn.password != "passwd"
    assert conn.addresses == ["192.168.1.1"]

    assert conn.instance_url == "kafka://some_cluster"


def test_kafka_empty_addresses(spark_mock):
    with pytest.raises(ValueError, match=re.escape("Passed empty parameter 'addresses'")):
        Kafka(spark=spark_mock, password="passwd", user="user", cluster="some_cluster", addresses=[])


def test_kafka_wrong_keytab_error(spark_mock, tmp_path_factory):
    # Arrange
    path = Path(tmp_path_factory.mktemp("data") / "keytab")
    path.write_text("content")
    os.chmod(path, 0o000)  # noqa: S103, WPS339

    # Assert
    with pytest.raises(
        ValueError,
        match=re.escape("The file does not exist or the user does not have read permissions"),
    ):
        Kafka(
            spark=spark_mock,
            keytab=path,
            user="user",
            cluster="some_cluster",
            addresses=["192.168.1.1"],
        )


def test_kafka_wrong_auth(spark_mock, tmp_path_factory):
    # Arrange
    path = Path(tmp_path_factory.mktemp("data") / "keytab")
    path.write_text("content")

    # Assert
    with pytest.raises(
        ValueError,
        match=re.escape("Authorization parameters passed at the same time, only two must be specified"),
    ):
        Kafka(
            spark=spark_mock,
            password="passwd",
            user="user",
            cluster="some_cluster",
            addresses=["192.168.1.1"],
            keytab=path,
        )

    with pytest.raises(
        ValueError,
        match=re.escape("Invalid parameters user, password or keytab passed."),
    ):
        Kafka(
            spark=spark_mock,
            password="passwd",
            cluster="some_cluster",
            addresses=["192.168.1.1"],
            keytab="path",
        )

    with pytest.raises(
        ValueError,
        match=re.escape("Invalid parameters user, password or keytab passed."),
    ):
        Kafka(
            spark=spark_mock,
            cluster="some_cluster",
            addresses=["192.168.1.1"],
            keytab="path",
        )

    with pytest.raises(
        ValueError,
        match=re.escape("Invalid parameters user, password or keytab passed."),
    ):
        Kafka(
            spark=spark_mock,
            password="passwd",
            cluster="some_cluster",
            addresses=["192.168.1.1"],
        )

    with pytest.raises(
        ValueError,
        match=re.escape("Invalid parameters user, password or keytab passed."),
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
        match=re.escape("1 validation error for Kafka\ncluster\n  field required (type=value_error.missing)"),
    ):
        Kafka(
            spark=spark_mock,
            password="passwd",
            user="user",
            addresses=["192.168.1.1"],
        )
