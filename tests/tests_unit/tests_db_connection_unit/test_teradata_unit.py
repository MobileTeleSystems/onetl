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


@pytest.mark.parametrize(
    "package_version, expected_package",
    [
        (None, ["com.teradata.jdbc:terajdbc:17.20.00.15"]),
        ("17.20.00.15", ["com.teradata.jdbc:terajdbc:17.20.00.15"]),
        ("16.20.00.13", ["com.teradata.jdbc:terajdbc:16.20.00.13"]),
    ],
)
def test_teradata_get_packages_valid_versions(package_version, expected_package):
    assert Teradata.get_packages(package_version=package_version) == expected_package


@pytest.mark.parametrize(
    "package_version",
    [
        "20.00.13",
        "abc",
    ],
)
def test_teradata_get_packages_invalid_version(package_version):
    with pytest.raises(
        ValueError,
        match=rf"Version '{package_version}' does not have enough numeric components for requested format \(expected at least 4\).",
    ):
        Teradata.get_packages(package_version=package_version)


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


def test_teradata_spark_stopped(spark_stopped):
    msg = "Spark session is stopped. Please recreate Spark session."
    with pytest.raises(ValueError, match=msg):
        Teradata(
            host="some_host",
            user="user",
            database="database",
            password="passwd",
            spark=spark_stopped,
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
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "com.teradata.jdbc.TeraDriver",
        "url": conn.jdbc_url,
    }

    assert "passwd" not in repr(conn)

    assert conn.instance_url == "teradata://some_host:1025"
    assert str(conn) == "Teradata[some_host:1025]"


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
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "com.teradata.jdbc.TeraDriver",
        "url": conn.jdbc_url,
    }

    assert conn.instance_url == "teradata://some_host:5000"
    assert str(conn) == "Teradata[some_host:5000]"


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
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "com.teradata.jdbc.TeraDriver",
        "url": conn.jdbc_url,
    }


def test_teradata_with_extra(spark_mock):
    conn = Teradata(
        host="some_host",
        user="user",
        password="passwd",
        database="database",
        extra={"TMODE": "TERA", "LOGMECH": "LDAP", "PARAM_WITH_COMMA": "some,value"},
        spark=spark_mock,
    )

    assert conn.jdbc_url == (
        "jdbc:teradata://some_host/CHARSET=UTF8,COLUMN_NAME=ON,DATABASE=database,"
        "DBS_PORT=1025,FLATTEN=ON,LOGMECH=LDAP,MAYBENULL=ON,PARAM_WITH_COMMA='some,value',STRICT_NAMES=OFF,TMODE=TERA"
    )
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "com.teradata.jdbc.TeraDriver",
        "url": conn.jdbc_url,
    }

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
    assert conn.jdbc_params == {
        "user": "user",
        "password": "passwd",
        "driver": "com.teradata.jdbc.TeraDriver",
        "url": conn.jdbc_url,
    }


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
