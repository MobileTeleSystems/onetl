import logging

import pytest

pytestmark = [pytest.mark.sftp, pytest.mark.file_connection, pytest.mark.connection]


def test_sftp_check(sftp_connection, caplog):
    with caplog.at_level(logging.INFO):
        assert sftp_connection.check() == sftp_connection

    assert "type = SFTP" in caplog.text
    assert f"host = '{sftp_connection.host}'" in caplog.text
    assert f"port = {sftp_connection.port}" in caplog.text
    assert f"user = '{sftp_connection.user}'" in caplog.text
    assert "timeout = 10" in caplog.text
    assert "host_key_check = False" in caplog.text
    assert "compress = True" in caplog.text
    assert "key_file" not in caplog.text
    assert "password = SecretStr('**********')" in caplog.text
    assert sftp_connection.password.get_secret_value() not in caplog.text

    assert "Connection is available" in caplog.text


def test_sftp_wrong_source_check():
    from onetl.connection import SFTP

    sftp = SFTP(user="some_user", password="pwd", host="host", port=123)

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        sftp.check()
