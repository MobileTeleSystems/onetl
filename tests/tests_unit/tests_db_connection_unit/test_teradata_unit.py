from unittest.mock import Mock

from onetl.connection import Teradata

spark = Mock()


def test_teradata_driver_and_uri():
    conn = Teradata(
        host="some_host",
        user="user",
        password="passwd",
        extra={"TMODE": "TERA", "LOGMECH": "LDAP"},
        spark=spark,
        database="default",
    )

    assert conn.jdbc_url == "jdbc:teradata://some_host/TMODE=TERA,LOGMECH=LDAP,DATABASE=default,DBS_PORT=1025"
    assert Teradata.driver == "com.teradata.jdbc.TeraDriver"
    assert Teradata.package == "com.teradata.jdbc:terajdbc4:17.10.00.25"
    assert Teradata.port == 1025


def test_teradata_without_database():
    conn = Teradata(
        host="some_host",
        user="user",
        password="passwd",
        extra={"TMODE": "TERA", "LOGMECH": "LDAP"},
        spark=spark,
    )

    assert conn.jdbc_url == "jdbc:teradata://some_host/TMODE=TERA,LOGMECH=LDAP,DBS_PORT=1025"
