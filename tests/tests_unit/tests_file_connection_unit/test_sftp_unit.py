from onetl.connection import SFTP, FileConnection


def test_sftp_connection():
    sftp = SFTP(host="some_host", user="some_user", password="pwd")
    assert isinstance(sftp, FileConnection)
    assert sftp.port == 22
