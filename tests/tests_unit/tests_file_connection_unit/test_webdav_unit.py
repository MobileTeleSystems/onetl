import pytest

from onetl.connection import FileConnection

pytestmark = [pytest.mark.webdav, pytest.mark.file_connection, pytest.mark.connection]


def test_webdav_connection():
    from onetl.connection import WebDAV

    webdav = WebDAV(host="some_host", user="some_user", password="pwd")
    assert isinstance(webdav, FileConnection)
    assert webdav.host == "some_host"
    assert webdav.protocol == "https"
    assert webdav.port == 443
    assert webdav.user == "some_user"
    assert webdav.password != "pwd"
    assert webdav.password.get_secret_value() == "pwd"

    assert "password='pwd'" not in str(webdav)
    assert "password='pwd'" not in repr(webdav)


def test_webdav_connection_with_http():
    from onetl.connection import WebDAV

    webdav = WebDAV(host="some_host", user="some_user", password="pwd", protocol="http")
    assert webdav.protocol == "http"
    assert webdav.port == 80


@pytest.mark.parametrize("protocol", ["http", "https"])
def test_webdav_connection_with_custom_port(protocol):
    from onetl.connection import WebDAV

    webdav = WebDAV(host="some_host", user="some_user", password="pwd", port=500, protocol=protocol)
    assert webdav.protocol == protocol
    assert webdav.port == 500


def test_webdav_connection_without_mandatory_args():
    from onetl.connection import WebDAV

    with pytest.raises(ValueError):
        WebDAV()
