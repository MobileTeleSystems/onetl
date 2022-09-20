import pytest

from onetl.connection import FTP, FileConnection


def test_ftp_connection():
    ftp = FTP(host="some_host", user="some_user", password="pwd")
    assert isinstance(ftp, FileConnection)
    assert ftp.host == "some_host"
    assert ftp.user == "some_user"
    assert ftp.password != "pwd"
    assert ftp.password.get_secret_value() == "pwd"
    assert ftp.port == 21


def test_ftp_connection_with_port():
    ftp = FTP(host="some_host", user="some_user", password="pwd", port=500)
    assert ftp.host == "some_host"
    assert ftp.user == "some_user"
    assert ftp.password != "pwd"
    assert ftp.password.get_secret_value() == "pwd"
    assert ftp.port == 500


def test_ftp_connection_without_mandatory_args():
    with pytest.raises(ValueError):
        FTP()
