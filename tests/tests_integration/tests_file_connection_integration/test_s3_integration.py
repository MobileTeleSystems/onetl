import logging

import pytest

pytestmark = pytest.mark.s3


def test_s3_connection_check(caplog, s3_connection):
    with caplog.at_level(logging.INFO):
        assert s3_connection.check() == s3_connection


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
