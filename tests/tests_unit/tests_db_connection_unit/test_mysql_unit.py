from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from onetl.connection import MySQL

spark = Mock(spec=SparkSession)


def test_mysql_class_attributes():
    assert MySQL.driver == "com.mysql.jdbc.Driver"
    assert MySQL.package == "mysql:mysql-connector-java:8.0.30"


def test_mysql():
    conn = MySQL(host="some_host", user="user", database="database", password="passwd", spark=spark)

    assert conn.host == "some_host"
    assert conn.port == 3306
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:mysql://some_host:3306/database?characterEncoding=UTF-8&useUnicode=yes"


def test_mysql_with_port():
    conn = MySQL(host="some_host", port=5000, user="user", database="database", password="passwd", spark=spark)

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:mysql://some_host:5000/database?characterEncoding=UTF-8&useUnicode=yes"


def test_mysql_without_database():
    conn = MySQL(host="some_host", user="user", password="passwd", spark=spark)

    assert conn.host == "some_host"
    assert conn.port == 3306
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert not conn.database

    assert conn.jdbc_url == "jdbc:mysql://some_host:3306?characterEncoding=UTF-8&useUnicode=yes"


def test_mysql_with_extra():
    conn = MySQL(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={"allowMultiQueries": "true", "requireSSL": "true"},
        spark=spark,
    )

    assert conn.jdbc_url == (
        "jdbc:mysql://some_host:3306/database?allowMultiQueries=true&characterEncoding=UTF-8&"
        "requireSSL=true&useUnicode=yes"
    )

    conn = MySQL(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={"characterEncoding": "CP-1251", "useUnicode": "no"},
        spark=spark,
    )

    assert conn.jdbc_url == ("jdbc:mysql://some_host:3306/database?characterEncoding=CP-1251&useUnicode=no")


def test_mysql_without_mandatory_args():
    with pytest.raises(ValueError, match="field required"):
        MySQL()

    with pytest.raises(ValueError, match="field required"):
        MySQL(
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        MySQL(
            host="some_host",
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        MySQL(
            host="some_host",
            user="user",
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        MySQL(
            host="some_host",
            password="passwd",
            spark=spark,
        )
