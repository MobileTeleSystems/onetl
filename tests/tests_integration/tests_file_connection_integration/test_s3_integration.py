import logging

import pytest

pytestmark = [pytest.mark.s3, pytest.mark.file_connection, pytest.mark.connection]


def test_s3_connection_check(caplog, s3_connection):
    with caplog.at_level(logging.INFO):
        assert s3_connection.check() == s3_connection

    assert "type = S3" in caplog.text
    assert f"host = '{s3_connection.host}'" in caplog.text
    assert f"port = {s3_connection.port}" in caplog.text
    assert f"bucket = '{s3_connection.bucket}'" in caplog.text
    assert f"access_key = '{s3_connection.access_key}'" in caplog.text
    assert "secret_key = SecretStr('**********')" in caplog.text
    assert s3_connection.secret_key.get_secret_value() not in caplog.text
    assert "session_token =" not in caplog.text

    assert "Connection is available" in caplog.text


def test_s3_wrong_source_check(s3_server):
    from onetl.connection import S3

    anonymous_connection = S3(
        host=s3_server.host,
        port=s3_server.port,
        bucket=s3_server.bucket,
        protocol=s3_server.protocol,
        access_key="unknown",
        secret_key="unknown",
    )

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        anonymous_connection.check()


def test_s3_connection_list_dir(s3_connection):
    s3_connection.client.fput_object(
        s3_connection.bucket,
        "export/resources/src/exclude_dir/file_4.txt",
        "tests/resources/src/exclude_dir/file_4.txt",
    )
    s3_connection.client.fput_object(
        s3_connection.bucket,
        "export/resources/src/exclude_dir/file_5.txt",
        "tests/resources/src/exclude_dir/file_5.txt",
    )

    assert [str(file) for file in s3_connection.listdir(directory="export/resources/src/exclude_dir")] == [
        "file_4.txt",
        "file_5.txt",
    ]
