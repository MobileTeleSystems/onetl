import pytest

from onetl.connection import FTPS, FileConnection


def test_ftps_connection():
    ftps = FTPS(host="some_host", user="some_user", password="pwd")
    assert isinstance(ftps, FileConnection)
    assert ftps.host == "some_host"
    assert ftps.user == "some_user"
    assert ftps.password == "pwd"
    assert ftps.port == 21


def test_ftps_connection_with_port():
    ftps = FTPS(host="some_host", user="some_user", password="pwd", port=500)
    assert ftps.host == "some_host"
    assert ftps.user == "some_user"
    assert ftps.password == "pwd"
    assert ftps.port == 500


def test_ftps_connection_without_mandatory_args():
    with pytest.raises(TypeError):
        FTPS()

    with pytest.raises(TypeError):
        FTPS(host="some_host")
