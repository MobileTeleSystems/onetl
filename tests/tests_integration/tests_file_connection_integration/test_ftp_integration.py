import logging

import pytest

pytestmark = [pytest.mark.ftp, pytest.mark.file_connection, pytest.mark.connection]


def test_ftp_check(ftp_connection, caplog):
    with caplog.at_level(logging.INFO):
        assert ftp_connection.check() == ftp_connection

    assert "type = FTP" in caplog.text
    assert f"host = '{ftp_connection.host}'" in caplog.text
    assert f"port = {ftp_connection.port}" in caplog.text
    assert f"user = '{ftp_connection.user}'" in caplog.text
    assert "password = SecretStr('**********')" in caplog.text
    assert ftp_connection.password.get_secret_value() not in caplog.text

    assert "Connection is available" in caplog.text


def test_ftp_check_anonymous(ftp_server, caplog):
    from onetl.connection import FTP

    anonymous_connection = FTP(host=ftp_server.host, port=ftp_server.port)

    with caplog.at_level(logging.INFO):
        assert anonymous_connection.check() == anonymous_connection

    assert "type = FTP" in caplog.text
    assert f"host = '{anonymous_connection.host}'" in caplog.text
    assert f"port = {anonymous_connection.port}" in caplog.text
    assert "user = " not in caplog.text
    assert "password = " not in caplog.text

    assert "Connection is available" in caplog.text


def test_ftp_wrong_source_check(ftp_server):
    from onetl.connection import FTP

    ftp = FTP(host=ftp_server.host, port=ftp_server.port, user="unknown", password="unknown")

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        ftp.check()
