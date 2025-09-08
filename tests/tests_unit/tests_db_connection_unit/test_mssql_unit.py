import re

import pytest

from onetl import __version__ as onetl_version
from onetl.connection import MSSQL

pytestmark = [pytest.mark.mssql, pytest.mark.db_connection, pytest.mark.connection]


def test_mssql_class_attributes():
    assert MSSQL.DRIVER == "com.microsoft.sqlserver.jdbc.SQLServerDriver"


def test_mssql_package():
    warning_msg = re.escape("will be removed in 1.0.0, use `MSSQL.get_packages()` instead")
    with pytest.warns(UserWarning, match=warning_msg):
        assert MSSQL.package == "com.microsoft.sqlserver:mssql-jdbc:13.2.0.jre8"


@pytest.mark.parametrize(
    "java_version, package_version, expected_packages",
    [
        (None, None, ["com.microsoft.sqlserver:mssql-jdbc:13.2.0.jre8"]),
        ("8", None, ["com.microsoft.sqlserver:mssql-jdbc:13.2.0.jre8"]),
        ("9", None, ["com.microsoft.sqlserver:mssql-jdbc:13.2.0.jre8"]),
        ("11", None, ["com.microsoft.sqlserver:mssql-jdbc:13.2.0.jre11"]),
        ("20", None, ["com.microsoft.sqlserver:mssql-jdbc:13.2.0.jre11"]),
        ("8", "13.2.0.jre8", ["com.microsoft.sqlserver:mssql-jdbc:13.2.0.jre8"]),
        ("11", "13.2.0.jre11", ["com.microsoft.sqlserver:mssql-jdbc:13.2.0.jre11"]),
        ("11", "12.7.0.jre11-preview", ["com.microsoft.sqlserver:mssql-jdbc:12.7.0.jre11-preview"]),
        ("8", "12.7.0.jre8-preview", ["com.microsoft.sqlserver:mssql-jdbc:12.7.0.jre8-preview"]),
        ("8", "13.2.0", ["com.microsoft.sqlserver:mssql-jdbc:13.2.0.jre8"]),
        ("11", "13.2.0", ["com.microsoft.sqlserver:mssql-jdbc:13.2.0.jre11"]),
    ],
)
def test_mssql_get_packages(java_version, package_version, expected_packages):
    assert MSSQL.get_packages(java_version=java_version, package_version=package_version) == expected_packages


@pytest.mark.parametrize(
    "package_version",
    [
        "12.7",
        "abc",
    ],
)
def test_mssql_get_packages_invalid_version(package_version):
    with pytest.raises(
        ValueError,
        match=rf"Version '{package_version}' does not have enough numeric components for requested format \(expected at least 3\).",
    ):
        MSSQL.get_packages(package_version=package_version)


@pytest.mark.parametrize("java_version", ["7", "6"])
def test_mssql_get_packages_java_version_not_supported(java_version):
    with pytest.raises(ValueError, match=f"Java version must be at least 8, got {java_version}"):
        MSSQL.get_packages(java_version=java_version)


def test_mssql_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'com.microsoft.sqlserver.jdbc.SQLServerDriver'"
    with pytest.raises(ValueError, match=msg):
        MSSQL(
            host="some_host",
            user="user",
            database="database",
            password="passwd",
            spark=spark_no_packages,
        )


def test_mssql_spark_stopped(spark_stopped):
    msg = "Spark session is stopped. Please recreate Spark session."
    with pytest.raises(ValueError, match=msg):
        MSSQL(
            host="some_host",
            user="user",
            database="database",
            password="passwd",
            spark=spark_stopped,
        )


def test_mssql(spark_mock):
    conn = MSSQL(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port is None
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:sqlserver://some_host"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "url": "jdbc:sqlserver://some_host",
        "databaseName": "database",
        "applicationName": f"local-123 abc onETL/{onetl_version} Spark/{spark_mock.version}",
    }

    assert "passwd" not in repr(conn)

    assert conn.instance_url == "mssql://some_host:1433/database"
    assert str(conn) == "MSSQL[some_host:1433/database]"


def test_mssql_with_custom_port(spark_mock):
    conn = MSSQL(host="some_host", port=5000, user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:sqlserver://some_host:5000"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "url": "jdbc:sqlserver://some_host:5000",
        "databaseName": "database",
        "applicationName": f"local-123 abc onETL/{onetl_version} Spark/{spark_mock.version}",
    }

    assert conn.instance_url == "mssql://some_host:5000/database"
    assert str(conn) == "MSSQL[some_host:5000/database]"


def test_mssql_with_instance_name(spark_mock):
    conn = MSSQL(
        host="some_host",
        user="user",
        database="database",
        password="passwd",
        extra={"instanceName": "myinstance"},
        spark=spark_mock,
    )

    assert conn.host == "some_host"
    assert conn.port is None
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:sqlserver://some_host"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "url": "jdbc:sqlserver://some_host",
        "instanceName": "myinstance",
        "databaseName": "database",
        "applicationName": f"local-123 abc onETL/{onetl_version} Spark/{spark_mock.version}",
    }

    assert conn.instance_url == "mssql://some_host\\myinstance/database"
    assert str(conn) == "MSSQL[some_host\\myinstance/database]"


def test_mssql_without_database_error(spark_mock):
    with pytest.raises(ValueError, match="field required"):
        MSSQL(
            host="some_host",
            user="user",
            password="passwd",
            spark=spark_mock,
        )


def test_mssql_with_extra(spark_mock):
    conn = MSSQL(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={
            "characterEncoding": "UTF-8",
            "trustServerCertificate": "true",
            "applicationName": "override",
        },
        spark=spark_mock,
    )

    assert conn.jdbc_url == "jdbc:sqlserver://some_host"
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "url": "jdbc:sqlserver://some_host",
        "databaseName": "database",
        "applicationName": "override",
        "characterEncoding": "UTF-8",
        "trustServerCertificate": "true",
    }


def test_mssql_with_extra_prohibited(spark_mock):
    with pytest.raises(ValueError, match=r"Options \['databaseName'\] are not allowed to use in a MSSQLExtra"):
        MSSQL(
            host="some_host",
            user="user",
            password="passwd",
            database="database",
            extra={"databaseName": "abc"},
            spark=spark_mock,
        )


def test_mssql_without_mandatory_args(spark_mock):
    with pytest.raises(ValueError, match="field required"):
        MSSQL()

    with pytest.raises(ValueError, match="field required"):
        MSSQL(
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        MSSQL(
            host="some_host",
            database="database",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        MSSQL(
            host="some_host",
            database="database",
            user="user",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        MSSQL(
            host="some_host",
            database="database",
            password="passwd",
            spark=spark_mock,
        )
