from pathlib import PurePosixPath

import pytest

from onetl.impl import RemotePath


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_rm_dir_recursive(file_connection, upload_test_files, path_type):
    file_connection.rmdir(path_type("/export/news_parse/"), recursive=True)

    assert not file_connection.listdir("/export")


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_rmdir_non_empty(file_connection, upload_test_files, path_type):
    with pytest.raises(Exception):
        file_connection.rmdir(path_type("/export/news_parse/"))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_rmdir_fake_dir(file_connection, upload_test_files, path_type):
    # Does not raise Exception
    file_connection.rmdir(path_type("/some/fake/dir"))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_mkdir(file_connection, upload_test_files, path_type):
    file_connection.close()
    file_connection.mkdir(path_type("/some_dir"))
    file_connection.close()
    # `close` called twice is not an error
    file_connection.close()

    assert RemotePath("some_dir") in file_connection.listdir("/")


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_rename_file(file_connection, upload_test_files, path_type):
    with file_connection as connection:
        connection.rename_file(
            source_file_path=path_type("/export/news_parse/exclude_dir/file_5.txt"),
            target_file_path=path_type("/export/news_parse/exclude_dir/file_55.txt"),
        )

    list_dir = file_connection.listdir("/export/news_parse/exclude_dir/")

    assert RemotePath("file_55.txt") in list_dir
    assert RemotePath("file_5.txt") not in list_dir
