import re

import pytest

from onetl.connection import MySQL

pytestmark = [pytest.mark.mysql, pytest.mark.db_connection, pytest.mark.connection]


def test_mysql_class_attributes():
    assert MySQL.DRIVER == "com.mysql.cj.jdbc.Driver"


def test_mysql_package():
    warning_msg = re.escape("will be removed in 1.0.0, use `MySQL.get_packages()` instead")
    with pytest.warns(UserWarning, match=warning_msg):
        assert MySQL.package == "com.mysql:mysql-connector-j:8.0.33"


def test_mysql_get_packages():
    assert MySQL.get_packages() == ["com.mysql:mysql-connector-j:8.0.33"]


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


def test_mysql(spark_mock):
    conn = MySQL(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 3306
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:mysql://some_host:3306/database?characterEncoding=UTF-8&useUnicode=yes"

    assert "password='passwd'" not in str(conn)
    assert "password='passwd'" not in repr(conn)


def test_mysql_with_port(spark_mock):
    conn = MySQL(host="some_host", port=5000, user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:mysql://some_host:5000/database?characterEncoding=UTF-8&useUnicode=yes"


def test_mysql_without_database(spark_mock):
    conn = MySQL(host="some_host", user="user", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 3306
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert not conn.database

    assert conn.jdbc_url == "jdbc:mysql://some_host:3306?characterEncoding=UTF-8&useUnicode=yes"


def test_mysql_with_extra(spark_mock):
    conn = MySQL(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={"allowMultiQueries": "true", "requireSSL": "true"},
        spark=spark_mock,
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
        spark=spark_mock,
    )

    assert conn.jdbc_url == ("jdbc:mysql://some_host:3306/database?characterEncoding=CP-1251&useUnicode=no")


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
