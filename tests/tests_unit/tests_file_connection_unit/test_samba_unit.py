import pytest

from onetl.connection import FileConnection, Samba


def test_samba_connection():
    samba = Samba(host="some_host", user="some_user", password="pwd")
    assert isinstance(samba, FileConnection)
    assert samba.host == "some_host"
    assert samba.user == "some_user"
    assert samba.password != "pwd"
    assert samba.password.get_secret_value() == "pwd"
    assert samba.port == 445


def test_samba_connection_with_port():
    samba = Samba(host="some_host", user="some_user", password="pwd", port=500)
    assert samba.host == "some_host"
    assert samba.user == "some_user"
    assert samba.password != "pwd"
    assert samba.password.get_secret_value() == "pwd"
    assert samba.port == 500


def test_samba_connection_without_mandatory_args():
    with pytest.raises(ValueError):
        Samba()
