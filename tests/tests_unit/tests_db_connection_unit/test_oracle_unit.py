import re

import pytest

from onetl.connection import Oracle

pytestmark = [pytest.mark.oracle, pytest.mark.db_connection, pytest.mark.connection]


def test_oracle_class_attributes():
    assert Oracle.DRIVER == "oracle.jdbc.driver.OracleDriver"


def test_oracle_package():
    warning_msg = re.escape("will be removed in 1.0.0, use `Oracle.get_packages()` instead")
    with pytest.warns(UserWarning, match=warning_msg):
        assert Oracle.package == "com.oracle.database.jdbc:ojdbc8:23.2.0.0"


def test_oracle_get_packages_no_input():
    assert Oracle.get_packages() == ["com.oracle.database.jdbc:ojdbc8:23.2.0.0"]


@pytest.mark.parametrize("java_version", ["7", "6"])
def test_oracle_get_packages_java_version_not_supported(java_version):
    with pytest.raises(ValueError, match=f"Java version must be at least 8, got {java_version}"):
        Oracle.get_packages(java_version=java_version)


@pytest.mark.parametrize(
    "java_version, package",
    [
        ("8", "com.oracle.database.jdbc:ojdbc8:23.2.0.0"),
        ("9", "com.oracle.database.jdbc:ojdbc8:23.2.0.0"),
        ("11", "com.oracle.database.jdbc:ojdbc11:23.2.0.0"),
        ("17", "com.oracle.database.jdbc:ojdbc11:23.2.0.0"),
        ("20", "com.oracle.database.jdbc:ojdbc11:23.2.0.0"),
    ],
)
def test_oracle_get_packages(java_version, package):
    assert Oracle.get_packages(java_version=java_version) == [package]


def test_oracle_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'oracle.jdbc.driver.OracleDriver'"
    with pytest.raises(ValueError, match=msg):
        Oracle(
            host="some_host",
            user="user",
            sid="sid",
            password="passwd",
            spark=spark_no_packages,
        )


def test_oracle(spark_mock):
    conn = Oracle(host="some_host", user="user", sid="sid", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 1521
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.sid == "sid"

    assert conn.jdbc_url == "jdbc:oracle:thin:@some_host:1521:sid"

    assert "password='passwd'" not in str(conn)
    assert "password='passwd'" not in repr(conn)


def test_oracle_with_port(spark_mock):
    conn = Oracle(host="some_host", port=5000, user="user", sid="sid", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.sid == "sid"

    assert conn.jdbc_url == "jdbc:oracle:thin:@some_host:5000:sid"


def test_oracle_uri_with_service_name(spark_mock):
    conn = Oracle(host="some_host", user="user", password="passwd", service_name="service", spark=spark_mock)

    assert conn.jdbc_url == "jdbc:oracle:thin:@//some_host:1521/service"


def test_oracle_without_sid_and_service_name(spark_mock):
    with pytest.raises(ValueError, match="One of parameters ``sid``, ``service_name`` should be set, got none"):
        Oracle(host="some_host", user="user", password="passwd", spark=spark_mock)


def test_oracle_both_sid_and_service_name(spark_mock):
    with pytest.raises(ValueError, match="Only one of parameters ``sid``, ``service_name`` can be set, got both"):
        Oracle(
            host="some_host",
            user="user",
            password="passwd",
            spark=spark_mock,
            service_name="service",
            sid="sid",
        )


@pytest.mark.parametrize("kwargs", [{"sid": "sid"}, {"service_name": "service_name"}, {}])
def test_oracle_with_database_error(spark_mock, kwargs):
    with pytest.raises(ValueError, match="extra fields not permitted"):
        Oracle(
            host="some_host",
            user="user",
            password="passwd",
            spark=spark_mock,
            database="database",
            **kwargs,
        )


def test_oracle_with_extra(spark_mock):
    conn = Oracle(
        host="some_host",
        user="user",
        password="passwd",
        sid="sid",
        extra={"tcpKeepAlive": "false", "connectTimeout": "10"},
        spark=spark_mock,
    )

    assert conn.jdbc_url == "jdbc:oracle:thin:@some_host:1521:sid?connectTimeout=10&tcpKeepAlive=false"


def test_oracle_without_mandatory_args(spark_mock):
    with pytest.raises(ValueError, match="field required"):
        Oracle()

    with pytest.raises(ValueError, match="field required"):
        Oracle(
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Oracle(
            host="some_host",
            sid="sid",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Oracle(
            host="some_host",
            sid="sid",
            user="user",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Oracle(
            host="some_host",
            sid="sid",
            password="passwd",
            spark=spark_mock,
        )
