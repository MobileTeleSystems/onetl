import pytest

from onetl.connection import FileConnection

pytestmark = [pytest.mark.webdav, pytest.mark.file_connection, pytest.mark.connection]


def test_webdav_connection():
    from onetl.connection import WebDAV

    conn = WebDAV(host="some_host", user="some_user", password="pwd")
    assert isinstance(conn, FileConnection)
    assert conn.host == "some_host"
    assert conn.protocol == "https"
    assert conn.port == 443
    assert conn.user == "some_user"
    assert conn.password != "pwd"
    assert conn.password.get_secret_value() == "pwd"
    assert conn.instance_url == "webdav://some_host:443"
    assert str(conn) == "WebDAV[some_host:443]"

    assert "pwd" not in repr(conn)


def test_webdav_connection_with_http():
    from onetl.connection import WebDAV

    conn = WebDAV(host="some_host", user="some_user", password="pwd", protocol="http")
    assert conn.protocol == "http"
    assert conn.port == 80
    assert conn.instance_url == "webdav://some_host:80"
    assert str(conn) == "WebDAV[some_host:80]"


@pytest.mark.parametrize("protocol", ["http", "https"])
def test_webdav_connection_with_custom_port(protocol):
    from onetl.connection import WebDAV

    conn = WebDAV(host="some_host", user="some_user", password="pwd", port=500, protocol=protocol)
    assert conn.protocol == protocol
    assert conn.port == 500
    assert conn.instance_url == "webdav://some_host:500"
    assert str(conn) == "WebDAV[some_host:500]"


def test_webdav_connection_without_mandatory_args():
    from onetl.connection import WebDAV

    with pytest.raises(ValueError):
        WebDAV()
