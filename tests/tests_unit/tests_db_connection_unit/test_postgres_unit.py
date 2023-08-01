import re

import pytest

from onetl.connection import Postgres

pytestmark = [pytest.mark.postgres, pytest.mark.db_connection, pytest.mark.connection]


def test_postgres_class_attributes():
    assert Postgres.DRIVER == "org.postgresql.Driver"


def test_postgres_package():
    warning_msg = re.escape("will be removed in 1.0.0, use `Postgres.get_packages()` instead")
    with pytest.warns(UserWarning, match=warning_msg):
        assert Postgres.package == "org.postgresql:postgresql:42.6.0"


def test_postgres_get_packages():
    assert Postgres.get_packages() == ["org.postgresql:postgresql:42.6.0"]


def test_oracle_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'org.postgresql.Driver'"
    with pytest.raises(ValueError, match=msg):
        Postgres(
            host="some_host",
            user="user",
            database="database",
            password="passwd",
            spark=spark_no_packages,
        )


def test_postgres(spark_mock):
    conn = Postgres(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 5432
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:postgresql://some_host:5432/database?ApplicationName=abc"

    assert "password='passwd'" not in str(conn)
    assert "password='passwd'" not in repr(conn)


def test_postgres_with_port(spark_mock):
    conn = Postgres(host="some_host", port=5000, user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == "jdbc:postgresql://some_host:5000/database?ApplicationName=abc"


def test_postgres_without_database_error(spark_mock):
    with pytest.raises(ValueError, match="field required"):
        Postgres(host="some_host", port=5000, user="user", password="passwd", spark=spark_mock)


def test_postgres_with_extra(spark_mock):
    conn = Postgres(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={"ssl": "true", "autosave": "always"},
        spark=spark_mock,
    )

    assert conn.jdbc_url == "jdbc:postgresql://some_host:5432/database?ApplicationName=abc&autosave=always&ssl=true"


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
