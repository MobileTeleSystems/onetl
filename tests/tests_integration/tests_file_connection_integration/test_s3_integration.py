import logging

import pytest

pytestmark = pytest.mark.s3


def test_s3_connection_check(caplog, s3):
    with caplog.at_level(logging.INFO):
        assert s3.check() == s3


def test_s3_connection_list_dir(s3):
    s3.client.fput_object(
        s3.bucket,
        "export/resources/src/exclude_dir/file_4.txt",
        "tests/resources/src/exclude_dir/file_4.txt",
    )
    s3.client.fput_object(
        s3.bucket,
        "export/resources/src/exclude_dir/file_5.txt",
        "tests/resources/src/exclude_dir/file_5.txt",
    )

    assert [str(file) for file in s3.listdir(directory="export/resources/src/exclude_dir")] == [
        "file_4.txt",
        "file_5.txt",
    ]
