import re

import pytest

from onetl import __version__ as onetl_version
from onetl.connection import Postgres

pytestmark = [pytest.mark.postgres, pytest.mark.db_connection, pytest.mark.connection]


def test_postgres_class_attributes():
    assert Postgres.DRIVER == "org.postgresql.Driver"


def test_postgres_package():
    warning_msg = re.escape("will be removed in 1.0.0, use `Postgres.get_packages()` instead")
    with pytest.warns(UserWarning, match=warning_msg):
        assert Postgres.package == "org.postgresql:postgresql:42.7.7"


@pytest.mark.parametrize(
    "package_version, expected_packages",
    [
        (None, ["org.postgresql:postgresql:42.7.7"]),
        ("42.7.7", ["org.postgresql:postgresql:42.7.7"]),
        ("42.7.7-patch", ["org.postgresql:postgresql:42.7.7-patch"]),
        ("42.6.0", ["org.postgresql:postgresql:42.6.0"]),
    ],
)
def test_postgres_get_packages(package_version, expected_packages):
    assert Postgres.get_packages(package_version=package_version) == expected_packages


@pytest.mark.parametrize(
    "package_version",
    [
        "42.2",
        "abc",
    ],
)
def test_postgres_get_packages_invalid_version(package_version):
    with pytest.raises(
        ValueError,
        match=rf"Version '{package_version}' does not have enough numeric components for requested format \(expected at least 3\).",
    ):
        Postgres.get_packages(package_version=package_version)


def test_postgres_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'org.postgresql.Driver'"
    with pytest.raises(ValueError, match=msg):
        Postgres(
            host="some_host",
            user="user",
            database="database",
            password="passwd",
            spark=spark_no_packages,
        )


def test_postgres_spark_stopped(spark_stopped):
    msg = "Spark session is stopped. Please recreate Spark session."
    with pytest.raises(ValueError, match=msg):
        Postgres(
            host="some_host",
            user="user",
            database="database",
            password="passwd",
            spark=spark_stopped,
        )


def test_postgres(spark_mock):
    conn = Postgres(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 5432
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:postgresql://some_host:5432/database"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://some_host:5432/database",
        "ApplicationName": f"local-123 abc onETL/{onetl_version} Spark/{spark_mock.version}",
        "tcpKeepAlive": "true",
        "stringtype": "unspecified",
    }

    assert "passwd" not in repr(conn)

    assert conn.instance_url == "postgres://some_host:5432/database"
    assert str(conn) == "Postgres[some_host:5432/database]"


def test_postgres_with_port(spark_mock):
    conn = Postgres(host="some_host", port=5000, user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:postgresql://some_host:5000/database"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://some_host:5000/database",
        "ApplicationName": f"local-123 abc onETL/{onetl_version} Spark/{spark_mock.version}",
        "tcpKeepAlive": "true",
        "stringtype": "unspecified",
    }

    assert conn.instance_url == "postgres://some_host:5000/database"
    assert str(conn) == "Postgres[some_host:5000/database]"


def test_postgres_without_database_error(spark_mock):
    with pytest.raises(ValueError, match="field required"):
        Postgres(host="some_host", port=5000, user="user", password="passwd", spark=spark_mock)


def test_postgres_with_extra(spark_mock):
    conn = Postgres(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={
            "stringtype": "VARCHAR",
            "autosave": "always",
            "tcpKeepAlive": "false",
            "ApplicationName": "override",
            "ssl": "true",
        },
        spark=spark_mock,
    )

    assert conn.jdbc_url == "jdbc:postgresql://some_host:5432/database"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://some_host:5432/database",
        "stringtype": "VARCHAR",
        "autosave": "always",
        "tcpKeepAlive": "false",
        "ApplicationName": "override",
        "ssl": "true",
    }


def test_postgres_without_mandatory_args(spark_mock):
    with pytest.raises(ValueError, match="field required"):
        Postgres()

    with pytest.raises(ValueError, match="field required"):
        Postgres(
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Postgres(
            host="some_host",
            database="database",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Postgres(
            host="some_host",
            database="database",
            user="user",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Postgres(
            host="some_host",
            database="database",
            password="passwd",
            spark=spark_mock,
        )
