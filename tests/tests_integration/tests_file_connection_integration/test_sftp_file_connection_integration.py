import logging

import pytest

pytestmark = [pytest.mark.sftp, pytest.mark.file_connection, pytest.mark.connection]


def test_sftp_file_connection_check_success(sftp_file_connection, caplog):
    sftp = sftp_file_connection
    with caplog.at_level(logging.INFO):
        assert sftp.check() == sftp

    assert "|SFTP|" in caplog.text
    assert f"host = '{sftp.host}'" in caplog.text
    assert f"port = {sftp.port}" in caplog.text
    assert f"user = '{sftp.user}'" in caplog.text
    assert f"extra = {sftp.extra.model_dump(exclude_none=True)!r}" in caplog.text
    assert "key_file" not in caplog.text
    assert "password = SecretStr('**********')" in caplog.text
    assert sftp.password.get_secret_value() not in caplog.text

    assert "Connection is available." in caplog.text


def test_sftp_file_connection_check_failed(sftp_server):
    from onetl.connection import SFTP

    sftp = SFTP(host=sftp_server.host, port=sftp_server.port, user="unknown", password="unknown")

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        sftp.check()
