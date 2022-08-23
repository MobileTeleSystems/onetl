from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from onetl.connection import Oracle

spark = Mock(spec=SparkSession)


def test_oracle_class_attributes():
    assert Oracle.driver == "oracle.jdbc.driver.OracleDriver"
    assert Oracle.package == "com.oracle.database.jdbc:ojdbc8:21.6.0.0.1"


def test_oracle():
    conn = Oracle(host="some_host", user="user", sid="sid", password="passwd", spark=spark)

    assert conn.host == "some_host"
    assert conn.port == 1521
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.sid == "sid"

    assert conn.jdbc_url == "jdbc:oracle:thin:@some_host:1521:sid"


def test_oracle_with_port():
    conn = Oracle(host="some_host", port=5000, user="user", sid="sid", password="passwd", spark=spark)

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.sid == "sid"

    assert conn.jdbc_url == "jdbc:oracle:thin:@some_host:5000:sid"


def test_oracle_uri_with_service_name():
    conn = Oracle(host="some_host", user="user", password="passwd", service_name="service", spark=spark)

    assert conn.jdbc_url == "jdbc:oracle:thin:@//some_host:1521/service"


def test_oracle_without_sid_and_service_name():
    with pytest.raises(ValueError, match="One of parameters ``sid``, ``service_name`` should be set, got none"):
        Oracle(host="some_host", user="user", password="passwd", spark=spark)


def test_oracle_both_sid_and_service_name():
    with pytest.raises(ValueError, match="Only one of parameters ``sid``, ``service_name`` can be set, got both"):
        Oracle(
            host="some_host",
            user="user",
            password="passwd",
            spark=spark,
            service_name="service",
            sid="sid",
        )


@pytest.mark.parametrize("kwargs", [{"sid": "sid"}, {"service_name": "service_name"}, {}])
def test_oracle_with_database_error(kwargs):
    with pytest.raises(ValueError, match="extra fields not permitted"):
        Oracle(
            host="some_host",
            user="user",
            password="passwd",
            spark=spark,
            database="database",
            **kwargs,
        )


def test_oracle_with_extra():
    conn = Oracle(
        host="some_host",
        user="user",
        password="passwd",
        sid="sid",
        extra={"tcpKeepAlive": "false", "connectTimeout": "10"},
        spark=spark,
    )

    assert conn.jdbc_url == "jdbc:oracle:thin:@some_host:1521:sid?connectTimeout=10&tcpKeepAlive=false"


def test_oracle_without_mandatory_args():
    with pytest.raises(ValueError, match="field required"):
        Oracle()

    with pytest.raises(ValueError, match="field required"):
        Oracle(
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        Oracle(
            host="some_host",
            sid="sid",
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        Oracle(
            host="some_host",
            sid="sid",
            user="user",
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        Oracle(
            host="some_host",
            sid="sid",
            password="passwd",
            spark=spark,
        )
