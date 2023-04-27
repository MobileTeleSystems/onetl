import logging

import pytest

pytestmark = [pytest.mark.ftps, pytest.mark.file_connection, pytest.mark.connection]


def test_ftps_check(ftps_connection, caplog):
    with caplog.at_level(logging.INFO):
        assert ftps_connection.check() == ftps_connection

    assert "type = FTPS" in caplog.text
    assert f"host = '{ftps_connection.host}'" in caplog.text
    assert f"port = {ftps_connection.port}" in caplog.text
    assert f"user = '{ftps_connection.user}'" in caplog.text

    if ftps_connection.password:
        assert ftps_connection.password.get_secret_value() not in caplog.text

    assert "Connection is available" in caplog.text


def test_ftps_wrong_source_check():
    from onetl.connection import FTPS

    ftps = FTPS(user="some_user", password="pwd", host="host", port=123)

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        ftps.check()
