from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from onetl.connection import MSSQL

spark = Mock(spec=SparkSession)


def test_mssql_class_attributes():
    assert MSSQL.driver == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    assert MSSQL.package == "com.microsoft.sqlserver:mssql-jdbc:10.2.1.jre8"


def test_mssql():
    conn = MSSQL(host="some_host", user="user", database="database", password="passwd", spark=spark)

    assert conn.host == "some_host"
    assert conn.port == 1433
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:sqlserver://some_host:1433;databaseName=database"


def test_mssql_with_port():
    conn = MSSQL(host="some_host", port=5000, user="user", database="database", password="passwd", spark=spark)

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:sqlserver://some_host:5000;databaseName=database"


def test_mssql_without_database_error():
    with pytest.raises(ValueError, match="field required"):
        MSSQL(host="some_host", user="user", password="passwd", spark=spark, extra={"trustServerCertificate": "true"})


def test_mssql_with_extra():
    conn = MSSQL(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={"characterEncoding": "UTF-8", "trustServerCertificate": "true"},
        spark=spark,
    )

    assert (
        conn.jdbc_url
        == "jdbc:sqlserver://some_host:1433;characterEncoding=UTF-8;databaseName=database;trustServerCertificate=true"
    )


def test_mssql_with_extra_prohibited():
    with pytest.raises(ValueError, match="Option 'databaseName' is not allowed to use in a Extra"):
        MSSQL(
            host="some_host",
            user="user",
            password="passwd",
            database="database",
            extra={"databaseName": "abc"},
            spark=spark,
        )


def test_mssql_without_mandatory_args():
    with pytest.raises(ValueError, match="field required"):
        MSSQL()

    with pytest.raises(ValueError, match="field required"):
        MSSQL(
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        MSSQL(
            host="some_host",
            database="database",
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        MSSQL(
            host="some_host",
            database="database",
            user="user",
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        MSSQL(
            host="some_host",
            database="database",
            password="passwd",
            spark=spark,
        )
