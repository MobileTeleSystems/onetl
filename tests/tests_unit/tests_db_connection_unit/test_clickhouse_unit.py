import pytest

from onetl._util.version import Version
from onetl.connection import Clickhouse

pytestmark = [pytest.mark.clickhouse, pytest.mark.db_connection, pytest.mark.connection]


def test_clickhouse_driver():
    assert Clickhouse.DRIVER == "com.clickhouse.jdbc.ClickHouseDriver"


def test_clickhouse_package():
    expected_packages = "com.clickhouse:clickhouse-jdbc:0.6.0,com.clickhouse:clickhouse-http-client:0.6.0,org.apache.httpcomponents.client5:httpclient5:5.3.1"
    assert Clickhouse.package == expected_packages


@pytest.mark.parametrize(
    "package_version, apache_http_client_version, expected_packages",
    [
        (
            None,
            None,
            [
                "com.clickhouse:clickhouse-jdbc:0.6.0",
                "com.clickhouse:clickhouse-http-client:0.6.0",
                "org.apache.httpcomponents.client5:httpclient5:5.3.1",
            ],
        ),
        (
            "0.6.0-patch3",
            "5.3.1",
            [
                "com.clickhouse:clickhouse-jdbc:0.6.0-patch3",
                "com.clickhouse:clickhouse-http-client:0.6.0-patch3",
                "org.apache.httpcomponents.client5:httpclient5:5.3.1",
            ],
        ),
        (
            "0.4.0",
            "4.5.14",
            ["com.clickhouse:clickhouse-jdbc:0.4.0", "com.clickhouse:clickhouse-http-client:0.4.0"],
        ),  # No HTTP client should be included
        (
            "0.5.0",
            "4.5.14",
            [
                "com.clickhouse:clickhouse-jdbc:0.5.0",
                "com.clickhouse:clickhouse-http-client:0.5.0",
                "org.apache.httpcomponents.client5:httpclient5:4.5.14",
            ],
        ),
        (
            "0.6.0",
            "4.5.14",
            [
                "com.clickhouse:clickhouse-jdbc:0.6.0",
                "com.clickhouse:clickhouse-http-client:0.6.0",
                "org.apache.httpcomponents.client5:httpclient5:4.5.14",
            ],
        ),
        (
            Version("0.5.0"),
            Version("4.5.14"),
            [
                "com.clickhouse:clickhouse-jdbc:0.5.0",
                "com.clickhouse:clickhouse-http-client:0.5.0",
                "org.apache.httpcomponents.client5:httpclient5:4.5.14",
            ],
        ),
        (
            Version("0.6.0"),
            Version("4.5.14"),
            [
                "com.clickhouse:clickhouse-jdbc:0.6.0",
                "com.clickhouse:clickhouse-http-client:0.6.0",
                "org.apache.httpcomponents.client5:httpclient5:4.5.14",
            ],
        ),
    ],
)
def test_clickhouse_get_packages(package_version, apache_http_client_version, expected_packages):
    assert (
        Clickhouse.get_packages(package_version=package_version, apache_http_client_version=apache_http_client_version)
        == expected_packages
    )


@pytest.mark.parametrize(
    "package_version, apache_http_client_version",
    [
        ("0.7", "5.3.1"),
        ("1", "5.4.0"),
        ("a.b.c", "5.3.1"),
    ],
)
def test_clickhouse_invalid_version(package_version, apache_http_client_version):
    with pytest.raises(
        ValueError,
        match=rf"Version '{package_version}' does not have enough numeric components for requested format \(expected at least 3\).",
    ):
        Clickhouse.get_packages(package_version=package_version, apache_http_client_version=apache_http_client_version)


def test_clickhouse_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'com.clickhouse.jdbc.ClickHouseDriver'"
    with pytest.raises(ValueError, match=msg):
        Clickhouse(
            host="some_host",
            user="user",
            database="database",
            password="passwd",
            spark=spark_no_packages,
        )


def test_clickhouse_spark_stopped(spark_stopped):
    msg = "Spark session is stopped. Please recreate Spark session."
    with pytest.raises(ValueError, match=msg):
        Clickhouse(
            host="some_host",
            user="user",
            database="database",
            password="passwd",
            spark=spark_stopped,
        )


def test_clickhouse(spark_mock):
    conn = Clickhouse(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 8123
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:clickhouse://some_host:8123/database"

    assert "password='passwd'" not in str(conn)
    assert "password='passwd'" not in repr(conn)


def test_clickhouse_with_port(spark_mock):
    conn = Clickhouse(
        host="some_host",
        port=5000,
        user="user",
        database="database",
        password="passwd",
        spark=spark_mock,
    )

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:clickhouse://some_host:5000/database"


def test_clickhouse_without_database(spark_mock):
    conn = Clickhouse(host="some_host", user="user", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 8123
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert not conn.database

    assert conn.jdbc_url == "jdbc:clickhouse://some_host:8123"


def test_clickhouse_with_extra(spark_mock):
    conn = Clickhouse(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={"socket_timeout": "120000", "query": "SELECT%201%3B"},
        spark=spark_mock,
    )

    assert conn.jdbc_url == "jdbc:clickhouse://some_host:8123/database?query=SELECT%201%3B&socket_timeout=120000"


def test_clickhouse_without_mandatory_args(spark_mock):
    with pytest.raises(ValueError, match="field required"):
        Clickhouse()

    with pytest.raises(ValueError, match="field required"):
        Clickhouse(
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Clickhouse(
            host="some_host",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Clickhouse(
            host="some_host",
            user="user",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Clickhouse(
            host="some_host",
            password="passwd",
            spark=spark_mock,
        )
