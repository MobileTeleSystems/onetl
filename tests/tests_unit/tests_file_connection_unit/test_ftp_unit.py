from onetl.connection import FTP, FileConnection


def test_ftp_connection():
    ftp = FTP(host="some_host", user="some_user", password="pwd")
    assert isinstance(ftp, FileConnection)
    assert ftp.port == 21
