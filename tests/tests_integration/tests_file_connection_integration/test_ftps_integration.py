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
    assert "password = SecretStr('**********')" in caplog.text
    assert ftps_connection.password.get_secret_value() not in caplog.text

    assert "Connection is available" in caplog.text


def test_ftps_check_anonymous(ftps_server, caplog):
    from onetl.connection import FTPS

    anonymous_connection = FTPS(host=ftps_server.host, port=ftps_server.port)

    with caplog.at_level(logging.INFO):
        assert anonymous_connection.check() == anonymous_connection

    assert "type = FTP" in caplog.text
    assert f"host = '{anonymous_connection.host}'" in caplog.text
    assert f"port = {anonymous_connection.port}" in caplog.text
    assert "user = " not in caplog.text
    assert "password = " not in caplog.text

    assert "Connection is available" in caplog.text


def test_ftps_wrong_source_check(ftps_server):
    from onetl.connection import FTPS

    ftps = FTPS(host=ftps_server.host, port=ftps_server.port, user="unknown", password="unknown")

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        ftps.check()
