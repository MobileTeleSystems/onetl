import logging

import pytest

pytestmark = [pytest.mark.webdav, pytest.mark.file_connection, pytest.mark.connection]


def test_webdav_file_connection_check_success(webdav_file_connection, caplog):
    webdav = webdav_file_connection
    with caplog.at_level(logging.INFO):
        assert webdav.check() == webdav

    assert "|WebDAV|" in caplog.text
    assert f"host = '{webdav.host}'" in caplog.text
    assert f"port = {webdav.port}" in caplog.text
    assert f"protocol = '{webdav.protocol}'" in caplog.text
    assert f"ssl_verify = {webdav.ssl_verify}" in caplog.text
    assert f"user = '{webdav.user}'" in caplog.text
    assert "password = SecretStr('**********')" in caplog.text
    assert webdav.password.get_secret_value() not in caplog.text

    assert "Connection is available." in caplog.text


def test_webdav_file_connection_check_failed(webdav_server):
    from onetl.connection import WebDAV

    webdav = WebDAV(
        host=webdav_server.host,
        port=webdav_server.port,
        protocol=webdav_server.protocol,
        user="unknown",
        password="unknown",
    )

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        webdav.check()
