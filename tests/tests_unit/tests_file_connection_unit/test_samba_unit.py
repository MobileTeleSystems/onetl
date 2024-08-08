import pytest

from onetl.connection import FileConnection

pytestmark = [pytest.mark.samba, pytest.mark.file_connection, pytest.mark.connection]


def test_samba_connection():
    from onetl.connection import Samba

    conn = Samba(host="some_host", share="share_name", user="some_user", password="pwd")
    assert isinstance(conn, FileConnection)
    assert conn.host == "some_host"
    assert conn.port == 445
    assert conn.share == "share_name"
    assert conn.protocol == "SMB"
    assert conn.domain == ""
    assert conn.auth_type == "NTLMv2"
    assert conn.user == "some_user"
    assert conn.password != "pwd"
    assert conn.password.get_secret_value() == "pwd"

    assert conn.instance_url == "smb://some_host:445/share_name"
    assert str(conn) == "Samba[some_host:445/share_name]"

    assert "pwd" not in repr(conn)


def test_samba_connection_with_net_bios():
    from onetl.connection import Samba

    conn = Samba(host="some_host", share="share_name", user="some_user", password="pwd", protocol="NetBIOS")
    assert conn.protocol == "NetBIOS"
    assert conn.port == 139


@pytest.mark.parametrize("protocol", ["SMB", "NetBIOS"])
def test_samba_connection_with_custom_port(protocol):
    from onetl.connection import Samba

    conn = Samba(host="some_host", share="share_name", user="some_user", password="pwd", protocol=protocol, port=444)
    assert conn.protocol == protocol
    assert conn.port == 444


def test_samba_connection_without_mandatory_args():
    from onetl.connection import Samba

    with pytest.raises(ValueError):
        Samba()
