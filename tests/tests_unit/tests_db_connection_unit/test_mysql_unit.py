import re

import pytest

from onetl import __version__ as onetl_version
from onetl.connection import MySQL

pytestmark = [pytest.mark.mysql, pytest.mark.db_connection, pytest.mark.connection]


def test_mysql_class_attributes():
    assert MySQL.DRIVER == "com.mysql.cj.jdbc.Driver"


def test_mysql_package():
    warning_msg = re.escape("will be removed in 1.0.0, use `MySQL.get_packages()` instead")
    with pytest.warns(UserWarning, match=warning_msg):
        assert MySQL.package == "com.mysql:mysql-connector-j:9.4.0"


@pytest.mark.parametrize(
    "package_version, expected_packages",
    [
        (None, ["com.mysql:mysql-connector-j:9.4.0"]),
        ("9.4.0", ["com.mysql:mysql-connector-j:9.4.0"]),
        ("8.1.0", ["com.mysql:mysql-connector-j:8.1.0"]),
        ("8.0.33", ["com.mysql:mysql-connector-j:8.0.33"]),
    ],
)
def test_mysql_get_packages(package_version, expected_packages):
    assert MySQL.get_packages(package_version=package_version) == expected_packages


@pytest.mark.parametrize(
    "package_version",
    [
        "8.3",
        "abc",
    ],
)
def test_mysql_get_packages_invalid_version(package_version):
    with pytest.raises(
        ValueError,
        match=rf"Version '{package_version}' does not have enough numeric components for requested format \(expected at least 3\).",
    ):
        MySQL.get_packages(package_version=package_version)


def test_mysql_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'com.mysql.cj.jdbc.Driver'"
    with pytest.raises(ValueError, match=msg):
        MySQL(
            host="some_host",
            user="user",
            database="database",
            password="passwd",
            spark=spark_no_packages,
        )


def test_mysql_spark_stopped(spark_stopped):
    msg = "Spark session is stopped. Please recreate Spark session."
    with pytest.raises(ValueError, match=msg):
        MySQL(
            host="some_host",
            user="user",
            database="database",
            password="passwd",
            spark=spark_stopped,
        )


def test_mysql(spark_mock):
    conn = MySQL(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 3306
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:mysql://some_host:3306/database"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "com.mysql.cj.jdbc.Driver",
        "url": "jdbc:mysql://some_host:3306/database",
        "characterEncoding": "UTF-8",
        "useUnicode": "yes",
        "connectionAttributes": f"program_name:local-123 abc onETL/{onetl_version} Spark/{spark_mock.version}",
    }

    assert "passwd" not in repr(conn)

    assert conn.instance_url == "mysql://some_host:3306"
    assert str(conn) == "MySQL[some_host:3306]"


def test_mysql_with_port(spark_mock):
    conn = MySQL(host="some_host", port=5000, user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:mysql://some_host:5000/database"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "com.mysql.cj.jdbc.Driver",
        "url": "jdbc:mysql://some_host:5000/database",
        "characterEncoding": "UTF-8",
        "useUnicode": "yes",
        "connectionAttributes": f"program_name:local-123 abc onETL/{onetl_version} Spark/{spark_mock.version}",
    }

    assert conn.instance_url == "mysql://some_host:5000"
    assert str(conn) == "MySQL[some_host:5000]"


def test_mysql_without_database(spark_mock):
    conn = MySQL(host="some_host", user="user", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 3306
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert not conn.database

    assert conn.jdbc_url == "jdbc:mysql://some_host:3306"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "com.mysql.cj.jdbc.Driver",
        "url": "jdbc:mysql://some_host:3306",
        "characterEncoding": "UTF-8",
        "useUnicode": "yes",
        "connectionAttributes": f"program_name:local-123 abc onETL/{onetl_version} Spark/{spark_mock.version}",
    }

    assert conn.instance_url == "mysql://some_host:3306"
    assert str(conn) == "MySQL[some_host:3306]"


def test_mysql_with_extra(spark_mock):
    conn = MySQL(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={
            "characterEncoding": "CP-1251",
            "useUnicode": "no",
            "connectionAttributes": "something:abc",
            "allowMultiQueries": "true",
            "requireSSL": "false",
        },
        spark=spark_mock,
    )
    assert conn.jdbc_url == "jdbc:mysql://some_host:3306/database"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "com.mysql.cj.jdbc.Driver",
        "url": "jdbc:mysql://some_host:3306/database",
        "characterEncoding": "CP-1251",
        "useUnicode": "no",
        "connectionAttributes": (
            f"something:abc,program_name:local-123 abc onETL/{onetl_version} Spark/{spark_mock.version}"
        ),
        "allowMultiQueries": "true",
        "requireSSL": "false",
    }

    conn = MySQL(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={"connectionAttributes": "something:abc,program_name:override"},
        spark=spark_mock,
    )
    assert conn.jdbc_url == "jdbc:mysql://some_host:3306/database"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "com.mysql.cj.jdbc.Driver",
        "url": "jdbc:mysql://some_host:3306/database",
        "characterEncoding": "UTF-8",
        "useUnicode": "yes",
        "connectionAttributes": "something:abc,program_name:override",
    }


def test_mysql_without_mandatory_args(spark_mock):
    with pytest.raises(ValueError, match="field required"):
        MySQL()

    with pytest.raises(ValueError, match="field required"):
        MySQL(
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        MySQL(
            host="some_host",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        MySQL(
            host="some_host",
            user="user",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        MySQL(
            host="some_host",
            password="passwd",
            spark=spark_mock,
        )
