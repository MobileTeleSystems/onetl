from unittest.mock import Mock

from onetl.connection import MySQL

spark = Mock()


def test_mysql_driver_and_uri():
    conn = MySQL(host="some_host", user="user", database="default", password="passwd", spark=spark)

    assert conn.jdbc_url == "jdbc:mysql://some_host:3306/default?useUnicode=yes&characterEncoding=UTF-8"
    assert MySQL.driver == "com.mysql.jdbc.Driver"
    assert MySQL.package == "mysql:mysql-connector-java:8.0.26"
    assert MySQL.port == 3306


def test_mysql_without_database():
    conn = MySQL(
        host="some_host",
        user="user",
        password="passwd",
        spark=spark,
        extra={"allowMultiQueries": "true", "requireSSL": "true"},
    )

    assert conn.jdbc_url == (
        "jdbc:mysql://some_host:3306?allowMultiQueries=true&requireSSL=true" "&useUnicode=yes&characterEncoding=UTF-8"
    )
