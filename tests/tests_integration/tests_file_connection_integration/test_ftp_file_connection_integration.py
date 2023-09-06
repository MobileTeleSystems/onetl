import logging

import pytest

pytestmark = [pytest.mark.ftp, pytest.mark.file_connection, pytest.mark.connection]


def test_ftp_file_connection_check_success(ftp_file_connection, caplog):
    ftp = ftp_file_connection
    with caplog.at_level(logging.INFO):
        assert ftp.check() == ftp

    assert "|FTP|" in caplog.text
    assert f"host = '{ftp.host}'" in caplog.text
    assert f"port = {ftp.port}" in caplog.text
    assert f"user = '{ftp.user}'" in caplog.text
    assert "password = SecretStr('**********')" in caplog.text
    assert ftp.password.get_secret_value() not in caplog.text

    assert "Connection is available." in caplog.text


def test_ftp_file_connection_check_anonymous(ftp_server, caplog):
    from onetl.connection import FTP

    anonymous = FTP(host=ftp_server.host, port=ftp_server.port)

    with caplog.at_level(logging.INFO):
        assert anonymous.check() == anonymous

    assert "|FTP|" in caplog.text
    assert f"host = '{anonymous.host}'" in caplog.text
    assert f"port = {anonymous.port}" in caplog.text
    assert "user = " not in caplog.text
    assert "password = " not in caplog.text

    assert "Connection is available." in caplog.text


def test_ftp_file_connection_check_failed(ftp_server):
    from onetl.connection import FTP

    ftp = FTP(host=ftp_server.host, port=ftp_server.port, user="unknown", password="unknown")

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        ftp.check()
