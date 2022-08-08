from unittest.mock import Mock

from onetl.connection import Teradata

spark = Mock()


def test_teradata_driver_and_uri():
    conn = Teradata(
        host="some_host",
        user="user",
        password="passwd",
        spark=spark,
        database="default",
    )

    assert conn.jdbc_url == "jdbc:teradata://some_host/STRICT_NAMES=OFF,FLATTEN=ON,DATABASE=default,DBS_PORT=1025"
    assert Teradata.driver == "com.teradata.jdbc.TeraDriver"
    assert Teradata.package == "com.teradata.jdbc:terajdbc4:17.20.00.08"
    assert Teradata.port == 1025


def test_teradata_without_database():
    conn = Teradata(
        host="some_host",
        user="user",
        password="passwd",
        extra={"TMODE": "TERA", "LOGMECH": "LDAP"},
        spark=spark,
    )

    assert conn.jdbc_url == (
        "jdbc:teradata://some_host/STRICT_NAMES=OFF,FLATTEN=ON,TMODE=TERA,LOGMECH=LDAP,DBS_PORT=1025"
    )


def test_teradata_override_defaults():
    conn = Teradata(
        host="some_host",
        user="user",
        password="passwd",
        extra={"FLATTEN": "OFF", "STRICT_NAMES": "ON"},
        spark=spark,
        database="default",
    )

    assert conn.jdbc_url == "jdbc:teradata://some_host/STRICT_NAMES=ON,FLATTEN=OFF,DATABASE=default,DBS_PORT=1025"
