import pytest

from onetl.connection import SFTP


def test_sftp_wrong_source_check():
    sftp = SFTP(user="some_user", password="pwd", host="host", port=123)

    with pytest.raises(RuntimeError):
        sftp.check()
