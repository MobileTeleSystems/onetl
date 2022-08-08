import pytest

from onetl.connection import SFTP, FileConnection


def test_sftp_connection():
    sftp = SFTP(host="some_host", user="some_user", password="pwd")
    assert isinstance(sftp, FileConnection)
    assert sftp.host == "some_host"
    assert sftp.user == "some_user"
    assert sftp.password == "pwd"
    assert sftp.port == 22


def test_sftp_connection_with_port():
    sftp = SFTP(host="some_host", user="some_user", password="pwd", port=500)
    assert sftp.host == "some_host"
    assert sftp.user == "some_user"
    assert sftp.password == "pwd"
    assert sftp.port == 500


def test_sftp_connection_without_mandatory_args():
    with pytest.raises(TypeError):
        SFTP()

    with pytest.raises(TypeError):
        SFTP(host="some_host")
