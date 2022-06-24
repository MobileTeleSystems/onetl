import pytest

from onetl.connection import FTPS


class TestFTPS:
    def test_ftps_wrong_source_check(self):
        ftps = FTPS(user="some_user", password="pwd", host="host", port=123)

        with pytest.raises(RuntimeError):
            ftps.check()
