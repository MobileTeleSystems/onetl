import pytest

from onetl.connection import FTP


def test_ftp_wrong_source_check():
    ftp = FTP(user="some_user", password="pwd", host="host", port=123)

    with pytest.raises(RuntimeError):
        ftp.check()
