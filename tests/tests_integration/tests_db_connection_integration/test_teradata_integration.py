import logging
import secrets

import pytest

from onetl.connection import Teradata

pytestmark = pytest.mark.teradata


def test_teradata_connection_check(spark, mocker, caplog):
    host = "some.domain.com"
    port = 1234
    database = secrets.token_hex()
    user = secrets.token_hex()
    password = secrets.token_hex()

    mocker.patch.object(Teradata, "_query_optional_on_driver")

    teradata = Teradata(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        spark=spark,
    )

    with caplog.at_level(logging.INFO):
        assert teradata.check() == teradata

    assert "|Teradata|" in caplog.text
    assert f"host = '{host}'" in caplog.text
    assert f"port = {port}" in caplog.text
    assert f"database = '{database}" in caplog.text
    assert f"user = '{user}'" in caplog.text
    assert "password = SecretStr('**********')" in caplog.text
    assert password not in caplog.text

    assert "package = " not in caplog.text
    assert "spark = " not in caplog.text

    assert "Connection is available." in caplog.text


def test_teradata_connection_check_fail(spark):
    teradata = Teradata(host="host", user="some_user", password="pwd", database="somedb", spark=spark)

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        teradata.check()
