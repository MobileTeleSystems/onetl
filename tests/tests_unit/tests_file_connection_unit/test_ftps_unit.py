from onetl.connection import FTPS, FileConnection


def test_ftps_connection():
    ftps = FTPS(host="some_host", user="some_user", password="pwd")
    assert isinstance(ftps, FileConnection)
    assert ftps.port == 21
