from unittest.mock import Mock

from onetl.connection import Clickhouse

spark = Mock()


def test_clickhouse_driver_and_uri():
    conn = Clickhouse(host="some_host", user="user", database="default", password="passwd", spark=spark)

    assert conn.jdbc_url == "jdbc:clickhouse://some_host:8123/default"
    assert Clickhouse.driver == "ru.yandex.clickhouse.ClickHouseDriver"
    assert Clickhouse.package == "ru.yandex.clickhouse:clickhouse-jdbc:0.3.0"
    assert Clickhouse.port == 8123


def test_clickhouse_without_database():
    conn = Clickhouse(
        host="some_host",
        user="user",
        password="passwd",
        extra={"socket_timeout": "120000", "query": "SELECT%201%3B"},
        spark=spark,
    )

    assert conn.jdbc_url == "jdbc:clickhouse://some_host:8123?socket_timeout=120000&query=SELECT%201%3B"
