from unittest.mock import Mock

import pytest
from pyspark.sql import SparkSession

from onetl.connection import Teradata

spark = Mock(spec=SparkSession)


def test_teradata_class_attributes():
    assert Teradata.driver == "com.teradata.jdbc.TeraDriver"
    assert Teradata.package == "com.teradata.jdbc:terajdbc4:17.20.00.08"


def test_teradata():
    conn = Teradata(host="some_host", user="user", database="database", password="passwd", spark=spark)

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


def test_teradata_with_port():
    conn = Teradata(host="some_host", port=5000, user="user", database="database", password="passwd", spark=spark)

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


def test_teradata_without_database():
    conn = Teradata(host="some_host", user="user", password="passwd", spark=spark)

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


def test_teradata_with_extra():
    conn = Teradata(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={"TMODE": "TERA", "LOGMECH": "LDAP"},
        spark=spark,
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
        spark=spark,
    )

    assert conn.jdbc_url == (
        "jdbc:teradata://some_host/CHARSET=CP-1251,COLUMN_NAME=OFF,DATABASE=database,"
        "DBS_PORT=1025,FLATTEN=OFF,MAYBENULL=OFF,STRICT_NAMES=ON"
    )


def test_teradata_with_extra_prohibited():
    with pytest.raises(ValueError, match="Options 'DATABASE', 'DBS_PORT' are not allowed to use in a Extra"):
        Teradata(
            host="some_host",
            user="user",
            password="passwd",
            database="database",
            extra={"DATABASE": "SOME", "DBS_PORT": "123"},
            spark=spark,
        )


def test_teradata_without_mandatory_args():
    with pytest.raises(ValueError, match="field required"):
        Teradata()

    with pytest.raises(ValueError, match="field required"):
        Teradata(
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        Teradata(
            host="some_host",
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        Teradata(
            host="some_host",
            user="user",
            spark=spark,
        )

    with pytest.raises(ValueError, match="field required"):
        Teradata(
            host="some_host",
            password="passwd",
            spark=spark,
        )
