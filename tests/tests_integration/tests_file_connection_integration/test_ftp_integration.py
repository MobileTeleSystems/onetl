import logging

import pytest

from onetl.connection import FTP

pytestmark = pytest.mark.ftp


def test_ftp_check(ftp_connection, caplog):
    with caplog.at_level(logging.INFO):
        assert ftp_connection.check() == ftp_connection

    assert "type = FTP" in caplog.text
    assert f"host = '{ftp_connection.host}'" in caplog.text
    assert f"port = {ftp_connection.port}" in caplog.text
    assert f"user = '{ftp_connection.user}'" in caplog.text

    if ftp_connection.password:
        assert ftp_connection.password.get_secret_value() not in caplog.text

    assert "Connection is available" in caplog.text


def test_ftp_wrong_source_check():
    ftp = FTP(user="some_user", password="pwd", host="host", port=123)

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        ftp.check()
