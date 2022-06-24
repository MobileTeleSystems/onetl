from onetl.connection import FileConnection, Samba


def test_samba_connection():
    samba = Samba(host="some_host", user="some_user", password="pwd")
    assert isinstance(samba, FileConnection)
    assert samba.port == 445
