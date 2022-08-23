from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from onetl.connection import Clickhouse

spark = Mock(spec=SparkSession)


def test_clickhouse_class_attributes():
    assert Clickhouse.driver == "ru.yandex.clickhouse.ClickHouseDriver"
    assert Clickhouse.package == "ru.yandex.clickhouse:clickhouse-jdbc:0.3.2"


def test_clickhouse():
    conn = Clickhouse(host="some_host", user="user", database="database", password="passwd", spark=spark)

    assert conn.host == "some_host"
    assert conn.port == 8123
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:clickhouse://some_host:8123/database"


def test_clickhouse_with_port():
    conn = Clickhouse(host="some_host", port=5000, user="user", database="database", password="passwd", spark=spark)

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:clickhouse://some_host:5000/database"


def test_clickhouse_without_database():
    conn = Clickhouse(host="some_host", user="user", password="passwd", spark=spark)

    assert conn.host == "some_host"
    assert conn.port == 8123
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert not conn.database

    assert conn.jdbc_url == "jdbc:clickhouse://some_host:8123"


def test_clickhouse_with_extra():
    conn = Clickhouse(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={"socket_timeout": "120000", "query": "SELECT%201%3B"},
        spark=spark,
    )

    assert conn.jdbc_url == "jdbc:clickhouse://some_host:8123/database?query=SELECT%201%3B&socket_timeout=120000"


def test_clickhouse_without_mandatory_args():
    with pytest.raises(ValueError, match="field required"):
        Clickhouse()

    with pytest.raises(ValueError, match="field required"):
        Clickhouse(
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        Clickhouse(
            host="some_host",
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        Clickhouse(
            host="some_host",
            user="user",
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        Clickhouse(
            host="some_host",
            password="passwd",
            spark=spark,
        )
