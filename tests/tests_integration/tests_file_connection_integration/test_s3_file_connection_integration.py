import logging
import os

import pytest

pytestmark = [pytest.mark.s3, pytest.mark.file_connection, pytest.mark.connection]


def test_s3_file_connection_check_success(caplog, s3_file_connection):
    s3 = s3_file_connection
    with caplog.at_level(logging.INFO):
        assert s3.check() == s3

    assert "|S3|" in caplog.text
    assert f"host = '{s3.host}'" in caplog.text
    assert f"port = {s3.port}" in caplog.text
    assert f"protocol = '{s3.protocol}'" in caplog.text
    assert f"bucket = '{s3.bucket}'" in caplog.text
    assert f"access_key = '{s3.access_key}'" in caplog.text
    assert "secret_key = SecretStr('**********')" in caplog.text
    assert s3.secret_key.get_secret_value() not in caplog.text
    assert "session_token =" not in caplog.text

    assert "Connection is available." in caplog.text


def test_s3_file_connection_check_failed(s3_server):
    from onetl.connection import S3

    anonymous = S3(
        host=s3_server.host,
        port=s3_server.port,
        bucket=s3_server.bucket,
        protocol=s3_server.protocol,
        access_key="unknown",
        secret_key="unknown",
    )

    with pytest.raises(RuntimeError, match="Connection is unavailable"):
        anonymous.check()


@pytest.mark.parametrize("path_prefix", ["/", ""])
def test_s3_file_connection_list_dir(path_prefix, s3_file_connection_with_path_and_files):
    s3, _, _ = s3_file_connection_with_path_and_files

    def dir_content(path):
        return sorted(os.fspath(file) for file in s3.list_dir(path))

    assert dir_content(f"{path_prefix}data/exclude_dir") == ["excluded1.txt", "nested"]
    assert dir_content(f"{path_prefix}data") == ["ascii.txt", "exclude_dir", "nested", "some.csv", "utf-8.txt"]
    assert "data" in dir_content(path_prefix)  # "tmp" could present
