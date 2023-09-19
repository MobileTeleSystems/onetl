import pytest

from onetl.connection import FileConnection

pytestmark = [pytest.mark.samba, pytest.mark.file_connection, pytest.mark.connection]


def test_samba_connection():
    from onetl.connection import Samba

    samba = Samba(host="some_host", share="share_name", user="some_user", password="pwd")
    assert isinstance(samba, FileConnection)
    assert samba.host == "some_host"
    assert samba.protocol == "SMB"
    assert samba.domain == ""
    assert samba.auth_type == "NTLMv2"
    assert samba.port == 445
    assert samba.user == "some_user"
    assert samba.password != "pwd"
    assert samba.password.get_secret_value() == "pwd"

    assert "password='pwd'" not in str(samba)
    assert "password='pwd'" not in repr(samba)


def test_samba_connection_with_net_bios():
    from onetl.connection import Samba

    samba = Samba(host="some_host", share="share_name", user="some_user", password="pwd", protocol="NetBIOS")
    assert samba.protocol == "NetBIOS"
    assert samba.port == 139


@pytest.mark.parametrize("protocol", ["SMB", "NetBIOS"])
def test_samba_connection_with_custom_port(protocol):
    from onetl.connection import Samba

    samba = Samba(host="some_host", share="share_name", user="some_user", password="pwd", protocol=protocol, port=444)
    assert samba.protocol == protocol
    assert samba.port == 444


def test_samba_connection_without_mandatory_args():
    from onetl.connection import Samba

    with pytest.raises(ValueError):
        Samba()
