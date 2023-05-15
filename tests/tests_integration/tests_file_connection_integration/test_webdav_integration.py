import logging

import pytest

pytestmark = [pytest.mark.webdav, pytest.mark.file_connection, pytest.mark.connection]


def test_webdav_check(webdav_connection, caplog):
    with caplog.at_level(logging.INFO):
        assert webdav_connection.check() == webdav_connection

    assert "type = WebDAV" in caplog.text
    assert f"host = '{webdav_connection.host}'" in caplog.text
    assert f"port = {webdav_connection.port}" in caplog.text
    assert f"protocol = '{webdav_connection.protocol}'" in caplog.text
    assert f"ssl_verify = {webdav_connection.ssl_verify}" in caplog.text
    assert f"user = '{webdav_connection.user}'" in caplog.text
    assert "password = SecretStr('**********')" in caplog.text
    assert webdav_connection.password.get_secret_value() not in caplog.text

    assert "Connection is available" in caplog.text


def test_webdav_wrong_source_check(webdav_server):
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
