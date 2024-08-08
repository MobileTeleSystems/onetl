import pytest

from onetl.connection import FileConnection

pytestmark = [pytest.mark.ftps, pytest.mark.file_connection, pytest.mark.connection]


def test_ftps_connection():
    from onetl.connection import FTPS

    conn = FTPS(host="some_host", user="some_user", password="pwd")
    assert isinstance(conn, FileConnection)
    assert conn.host == "some_host"
    assert conn.user == "some_user"
    assert conn.password != "pwd"
    assert conn.password.get_secret_value() == "pwd"
    assert conn.port == 21

    assert str(conn) == "FTPS[some_host:21]"
    assert "pwd" not in repr(conn)


def test_ftps_connection_anonymous():
    from onetl.connection import FTPS

    conn = FTPS(host="some_host")

    assert isinstance(conn, FileConnection)
    assert conn.host == "some_host"
    assert conn.user is None
    assert conn.password is None


def test_ftps_connection_with_port():
    from onetl.connection import FTPS

    conn = FTPS(host="some_host", user="some_user", password="pwd", port=500)

    assert conn.port == 500
    assert str(conn) == "FTPS[some_host:500]"


def test_ftps_connection_without_mandatory_args():
    from onetl.connection import FTPS

    with pytest.raises(ValueError):
        FTPS()
