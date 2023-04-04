import pytest

from onetl.connection import MSSQL

pytestmark = pytest.mark.mssql


def test_mssql_class_attributes():
    assert MSSQL.driver == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    assert MSSQL.package == "com.microsoft.sqlserver:mssql-jdbc:10.2.1.jre8"


def test_mssql(spark_mock):
    conn = MSSQL(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 1433
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:sqlserver://some_host:1433;databaseName=database"


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
    with pytest.raises(ValueError, match=r"Options \['databaseName'\] are not allowed to use in a Extra"):
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
