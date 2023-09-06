import logging

import pytest

pytestmark = [pytest.mark.ftps, pytest.mark.file_connection, pytest.mark.connection]


def test_ftps_file_connection_check_success(ftps_file_connection, caplog):
    ftps = ftps_file_connection
    with caplog.at_level(logging.INFO):
        assert ftps.check() == ftps

    assert "|FTPS|" in caplog.text
    assert f"host = '{ftps.host}'" in caplog.text
    assert f"port = {ftps.port}" in caplog.text
    assert f"user = '{ftps.user}'" in caplog.text
    assert "password = SecretStr('**********')" in caplog.text
    assert ftps.password.get_secret_value() not in caplog.text

    assert "Connection is available." in caplog.text


def test_ftps_file_connection_check_anonymous(ftps_server, caplog):
    from onetl.connection import FTPS

    anonymous = FTPS(host=ftps_server.host, port=ftps_server.port)

    with caplog.at_level(logging.INFO):
        assert anonymous.check() == anonymous

    assert "|FTPS|" in caplog.text
    assert f"host = '{anonymous.host}'" in caplog.text
    assert f"port = {anonymous.port}" in caplog.text
    assert "user = " not in caplog.text
    assert "password = " not in caplog.text

    assert "Connection is available." in caplog.text


def test_ftps_file_connection_check_failed(ftps_server):
    from onetl.connection import FTPS

    ftps = FTPS(host=ftps_server.host, port=ftps_server.port, user="unknown", password="unknown")

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        ftps.check()
