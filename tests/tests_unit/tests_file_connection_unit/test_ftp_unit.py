import pytest

from onetl.connection import FileConnection

pytestmark = [pytest.mark.ftp, pytest.mark.file_connection, pytest.mark.connection]


def test_ftp_connection():
    from onetl.connection import FTP

    ftp = FTP(host="some_host", user="some_user", password="pwd")
    assert isinstance(ftp, FileConnection)
    assert ftp.host == "some_host"
    assert ftp.user == "some_user"
    assert ftp.password != "pwd"
    assert ftp.password.get_secret_value() == "pwd"
    assert ftp.port == 21

    assert "password='pwd'" not in str(ftp)
    assert "password='pwd'" not in repr(ftp)


def test_ftp_connection_anonymous():
    from onetl.connection import FTP

    ftp = FTP(host="some_host")

    assert isinstance(ftp, FileConnection)
    assert ftp.host == "some_host"
    assert ftp.user is None
    assert ftp.password is None


def test_ftp_connection_with_port():
    from onetl.connection import FTP

    ftp = FTP(host="some_host", user="some_user", password="pwd", port=500)

    assert ftp.port == 500


def test_ftp_connection_without_mandatory_args():
    from onetl.connection import FTP

    with pytest.raises(ValueError):
        FTP()
