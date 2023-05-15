import pytest

from onetl.connection import FileConnection

pytestmark = [pytest.mark.ftps, pytest.mark.file_connection, pytest.mark.connection]


def test_ftps_connection():
    from onetl.connection import FTPS

    ftps = FTPS(host="some_host", user="some_user", password="pwd")
    assert isinstance(ftps, FileConnection)
    assert ftps.host == "some_host"
    assert ftps.user == "some_user"
    assert ftps.password != "pwd"
    assert ftps.password.get_secret_value() == "pwd"
    assert ftps.port == 21

    assert "password='pwd'" not in str(ftps)
    assert "password='pwd'" not in repr(ftps)


def test_ftps_connection_anonymous():
    from onetl.connection import FTPS

    ftps = FTPS(host="some_host")

    assert isinstance(ftps, FileConnection)
    assert ftps.host == "some_host"
    assert ftps.user is None
    assert ftps.password is None


def test_ftps_connection_with_port():
    from onetl.connection import FTPS

    ftps = FTPS(host="some_host", user="some_user", password="pwd", port=500)

    assert ftps.port == 500


def test_ftps_connection_without_mandatory_args():
    from onetl.connection import FTPS

    with pytest.raises(ValueError):
        FTPS()
