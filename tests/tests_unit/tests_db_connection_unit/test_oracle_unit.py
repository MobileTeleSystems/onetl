from unittest.mock import Mock

import pytest

from onetl.connection import Oracle

spark = Mock()


def test_oracle_driver_and_uri():
    conn = Oracle(
        host="some_host",
        user="user",
        password="passwd",
        sid="PE",
        spark=spark,
        extra={"tcpKeepAlive": "false", "connectTimeout": "10"},
    )

    assert conn.jdbc_url == "jdbc:oracle:thin:@some_host:1521:PE?tcpKeepAlive=false&connectTimeout=10"
    assert Oracle.driver == "oracle.jdbc.driver.OracleDriver"
    assert Oracle.package == "com.oracle.database.jdbc:ojdbc8:21.6.0.0.1"
    assert Oracle.port == 1521


def test_oracle_uri_with_service_name():
    conn = Oracle(host="some_host", user="user", password="passwd", service_name="DWHLDTS", spark=spark)

    assert conn.jdbc_url == "jdbc:oracle:thin:@//some_host:1521/DWHLDTS"


def test_oracle_without_set_sid_service_name():
    with pytest.raises(ValueError):
        Oracle(host="some_host", user="user", password="passwd", spark=spark)


def test_oracle_set_sid_and_service_name():
    with pytest.raises(ValueError):
        Oracle(
            host="some_host",
            user="user",
            password="passwd",
            spark=spark,
            service_name="service",
            sid="sid",
        )
