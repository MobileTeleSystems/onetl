import re

import pytest

from onetl.connection import Teradata

pytestmark = [pytest.mark.teradata, pytest.mark.db_connection, pytest.mark.connection]


def test_teradata_class_attributes():
    assert Teradata.DRIVER == "com.teradata.jdbc.TeraDriver"


def test_teradata_package():
    warning_msg = re.escape("will be removed in 1.0.0, use `Teradata.get_packages()` instead")
    with pytest.warns(UserWarning, match=warning_msg):
        assert Teradata.package == "com.teradata.jdbc:terajdbc:17.20.00.15"


def test_teradata_get_packages():
    assert Teradata.get_packages() == ["com.teradata.jdbc:terajdbc:17.20.00.15"]


def test_teradata_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'com.teradata.jdbc.TeraDriver'"
    with pytest.raises(ValueError, match=msg):
        Teradata(
            host="some_host",
            user="user",
            database="database",
            password="passwd",
            spark=spark_no_packages,
        )


def test_teradata(spark_mock):
    conn = Teradata(host="some_host", user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 1025
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == (
        "jdbc:teradata://some_host/CHARSET=UTF8,COLUMN_NAME=ON,DATABASE=database,"
        "DBS_PORT=1025,FLATTEN=ON,MAYBENULL=ON,STRICT_NAMES=OFF"
    )

    assert "password='passwd'" not in str(conn)
    assert "password='passwd'" not in repr(conn)


def test_teradata_with_port(spark_mock):
    conn = Teradata(host="some_host", port=5000, user="user", database="database", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 5000
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert conn.database == "database"

    assert conn.jdbc_url == (
        "jdbc:teradata://some_host/CHARSET=UTF8,COLUMN_NAME=ON,DATABASE=database,"
        "DBS_PORT=5000,FLATTEN=ON,MAYBENULL=ON,STRICT_NAMES=OFF"
    )


def test_teradata_without_database(spark_mock):
    conn = Teradata(host="some_host", user="user", password="passwd", spark=spark_mock)

    assert conn.host == "some_host"
    assert conn.port == 1025
    assert conn.user == "user"
    assert conn.password != "passwd"
    assert conn.password.get_secret_value() == "passwd"
    assert not conn.database

    assert conn.jdbc_url == (
        "jdbc:teradata://some_host/CHARSET=UTF8,COLUMN_NAME=ON,"
        "DBS_PORT=1025,FLATTEN=ON,MAYBENULL=ON,STRICT_NAMES=OFF"
    )


def test_teradata_with_extra(spark_mock):
    conn = Teradata(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={"TMODE": "TERA", "LOGMECH": "LDAP"},
        spark=spark_mock,
    )

    assert conn.jdbc_url == (
        "jdbc:teradata://some_host/CHARSET=UTF8,COLUMN_NAME=ON,DATABASE=database,"
        "DBS_PORT=1025,FLATTEN=ON,LOGMECH=LDAP,MAYBENULL=ON,STRICT_NAMES=OFF,TMODE=TERA"
    )

    conn = Teradata(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={"FLATTEN": "OFF", "STRICT_NAMES": "ON", "COLUMN_NAME": "OFF", "MAYBENULL": "OFF", "CHARSET": "CP-1251"},
        spark=spark_mock,
    )

    assert conn.jdbc_url == (
        "jdbc:teradata://some_host/CHARSET=CP-1251,COLUMN_NAME=OFF,DATABASE=database,"
        "DBS_PORT=1025,FLATTEN=OFF,MAYBENULL=OFF,STRICT_NAMES=ON"
    )


def test_teradata_with_extra_prohibited(spark_mock):
    with pytest.raises(
        ValueError,
        match=r"Options \['DATABASE', 'DBS_PORT'\] are not allowed to use in a TeradataExtra",
    ):
        Teradata(
            host="some_host",
            user="user",
            password="passwd",
            database="database",
            extra={"DATABASE": "SOME", "DBS_PORT": "123"},
            spark=spark_mock,
        )


def test_teradata_without_mandatory_args(spark_mock):
    with pytest.raises(ValueError, match="field required"):
        Teradata()

    with pytest.raises(ValueError, match="field required"):
        Teradata(
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Teradata(
            host="some_host",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Teradata(
            host="some_host",
            user="user",
            spark=spark_mock,
        )

    with pytest.raises(ValueError, match="field required"):
        Teradata(
            host="some_host",
            password="passwd",
            spark=spark_mock,
        )
