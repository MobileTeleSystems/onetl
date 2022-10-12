import logging

import pytest


@pytest.mark.S3
def test_s3_connection_check(caplog, s3):
    with caplog.at_level(logging.INFO):
        assert s3.check() == s3


@pytest.mark.S3
def test_s3_connection_path_exists(s3):
    s3.client.fput_object(
        "testbucket",
        "export/resources/src/exclude_dir/file_4.txt",
        "tests/resources/src/exclude_dir/file_4.txt",
    )
    assert s3.path_exists("export/resources/src/exclude_dir")
    assert not s3.path_exists("export/resources/src/exclude_dir_32")


@pytest.mark.S3
def test_s3_connection_is_dir(s3):
    s3.client.fput_object(
        "testbucket",
        "export/resources/src/exclude_dir/file_4.txt",
        "tests/resources/src/exclude_dir/file_4.txt",
    )
    assert s3.is_dir("export")


@pytest.mark.S3
def test_s3_connection_list_dir(s3):
    s3.client.fput_object(
        "testbucket",
        "export/resources/src/exclude_dir/file_4.txt",
        "tests/resources/src/exclude_dir/file_4.txt",
    )
    s3.client.fput_object(
        "testbucket",
        "export/resources/src/exclude_dir/file_5.txt",
        "tests/resources/src/exclude_dir/file_5.txt",
    )

    assert [str(file) for file in s3.listdir(directory="export/resources/src/exclude_dir")] == [
        "file_4.txt",
        "file_5.txt",
    ]


@pytest.mark.S3
def test_s3_connection_is_file(s3):
    s3.client.fput_object(
        "testbucket",
        "export/resources/src/exclude_dir/file_4.txt",
        "tests/resources/src/exclude_dir/file_4.txt",
    )
    assert s3.is_file("export/resources/src/exclude_dir/file_4.txt")
    assert not s3.is_file("export/resources/src/exclude_dir")
