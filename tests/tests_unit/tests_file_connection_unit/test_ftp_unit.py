import pytest

from onetl.connection import FileConnection

pytestmark = [pytest.mark.ftp, pytest.mark.file_connection, pytest.mark.connection]


def test_ftp_connection():
    from onetl.connection import FTP

    conn = FTP(host="some_host", user="some_user", password="pwd")
    assert isinstance(conn, FileConnection)
    assert conn.host == "some_host"
    assert conn.user == "some_user"
    assert conn.password != "pwd"
    assert conn.password.get_secret_value() == "pwd"
    assert conn.port == 21

    assert str(conn) == "FTP[some_host:21]"
    assert "pwd" not in repr(conn)


def test_ftp_connection_anonymous():
    from onetl.connection import FTP

    conn = FTP(host="some_host")
    assert conn.host == "some_host"
    assert conn.user is None
    assert conn.password is None


def test_ftp_connection_with_port():
    from onetl.connection import FTP

    conn = FTP(host="some_host", user="some_user", password="pwd", port=500)

    assert conn.port == 500
    assert str(conn) == "FTP[some_host:500]"


def test_ftp_connection_without_mandatory_args():
    from onetl.connection import FTP

    with pytest.raises(ValueError):
        FTP()
