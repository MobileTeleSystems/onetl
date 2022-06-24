from unittest.mock import Mock

import pytest

from onetl.connection import MSSQL

spark = Mock()


def test_mssql_without_database_error():
    with pytest.raises(ValueError):
        MSSQL(host="some_host", user="user", password="passwd", spark=spark)


def test_mssql_driver_and_uri():
    conn = MSSQL(
        host="some_host",
        user="user",
        password="passwd",
        extra={"characterEncoding": "UTF-8"},
        spark=spark,
        database="default",
    )

    assert conn.jdbc_url == "jdbc:sqlserver://some_host:1433;databaseName=default;characterEncoding=UTF-8"
    assert MSSQL.driver == "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    assert MSSQL.package == "com.microsoft.sqlserver:mssql-jdbc:7.2.0.jre8"
    assert MSSQL.port == 1433
