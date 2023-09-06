import re

import pytest

from onetl.connection import MSSQL

pytestmark = [pytest.mark.mssql, pytest.mark.db_connection, pytest.mark.connection]


def test_mssql_class_attributes():
    assert MSSQL.DRIVER == "com.microsoft.sqlserver.jdbc.SQLServerDriver"


def test_mssql_package():
    warning_msg = re.escape("will be removed in 1.0.0, use `MSSQL.get_packages()` instead")
    with pytest.warns(UserWarning, match=warning_msg):
        assert MSSQL.package == "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8"


def test_mssql_get_packages_no_input():
    assert MSSQL.get_packages() == ["com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8"]


@pytest.mark.parametrize("java_version", ["7", "6"])
def test_mssql_get_packages_java_version_not_supported(java_version):
    with pytest.raises(ValueError, match=f"Java version must be at least 8, got {java_version}"):
        MSSQL.get_packages(java_version=java_version)


@pytest.mark.parametrize(
    "java_version, package",
    [
        ("8", "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8"),
        ("9", "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre8"),
        ("11", "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre11"),
        ("17", "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre11"),
        ("20", "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre11"),
    ],
)
def test_mssql_get_packages(java_version, package):
    assert MSSQL.get_packages(java_version=java_version) == [package]


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


def test_mssql(spark_mock):
    conn = MSSQL(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 1433
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:sqlserver://some_host:1433;databaseName=database"

    assert "password='passwd'" not in str(conn)
    assert "password='passwd'" not in repr(conn)


def test_mssql_with_port(spark_mock):
    conn = MSSQL(host="some_host", port=5000, user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:sqlserver://some_host:5000;databaseName=database"


def test_mssql_without_database_error(spark_mock):
    with pytest.raises(ValueError, match="field required"):
        MSSQL(
            host="some_host",
            user="user",
            password="passwd",
            spark=spark_mock,
            extra={"trustServerCertificate": "true"},
        )


def test_mssql_with_extra(spark_mock):
    conn = MSSQL(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={"characterEncoding": "UTF-8", "trustServerCertificate": "true"},
        spark=spark_mock,
    )

    assert (
        conn.jdbc_url
        == "jdbc:sqlserver://some_host:1433;characterEncoding=UTF-8;databaseName=database;trustServerCertificate=true"
    )


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
