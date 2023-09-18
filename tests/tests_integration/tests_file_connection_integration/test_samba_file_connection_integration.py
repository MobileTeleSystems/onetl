import logging

import pytest

pytestmark = [pytest.mark.samba, pytest.mark.file_connection, pytest.mark.connection]


def test_samba_file_connection_check_success(samba_file_connection, caplog):
    samba = samba_file_connection
    with caplog.at_level(logging.INFO):
        assert samba.check() == samba

    assert "|Samba|" in caplog.text
    assert f"host = '{samba.host}'" in caplog.text
    assert f"port = {samba.port}" in caplog.text
    assert f"protocol = '{samba.protocol}'" in caplog.text
    assert f"user = '{samba.user}'" in caplog.text
    assert f"share = '{samba.share}'" in caplog.text
    assert "timeout = 10" in caplog.text
    assert "password = SecretStr('**********')" in caplog.text
    assert samba.password.get_secret_value() not in caplog.text

    assert "Connection is available." in caplog.text


def test_samba_file_connection_check_failed(samba_server):
    from onetl.connection import Samba

    samba = Samba(
        host=samba_server.host,
        share=samba_server.share,
        protocol=samba_server.protocol,
        port=samba_server.port,
        user="unknown",
        password="unknown",
    )

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        samba.check()
