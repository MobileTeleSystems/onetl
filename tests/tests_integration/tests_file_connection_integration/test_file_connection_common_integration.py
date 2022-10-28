from pathlib import PurePosixPath

import pytest
from pytest_lazyfixture import lazy_fixture

from onetl.exception import NotAFileError, DirectoryNotFoundError
from onetl.impl import RemotePath


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_rm_dir_recursive_without_s3(
    file_connection_without_s3,
    upload_test_files_without_s3,
    path_type,
):
    file_connection_without_s3.rmdir(path_type("/export/news_parse/"), recursive=True)

    assert not file_connection_without_s3.listdir("/export")


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_rm_dir_recursive_s3(s3, upload_test_files_s3, path_type):
    s3.rmdir(path_type("/export/news_parse/"), recursive=True)

    with pytest.raises(DirectoryNotFoundError):
        s3.is_dir("/export")


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_rmdir_non_empty(file_all_connections, upload_test_files, path_type):
    with pytest.raises(Exception):
        file_all_connections.rmdir(path_type("/export/news_parse/"))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_rmdir_fake_dir(file_all_connections, upload_test_files, path_type):
    # Does not raise Exception
    file_all_connections.rmdir(path_type("/some/fake/dir"))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_mkdir(file_connection_without_s3, upload_test_files_without_s3, path_type):
    file_connection_without_s3.close()
    file_connection_without_s3.mkdir(path_type("/some_dir"))
    file_connection_without_s3.close()
    # `close` called twice is not an error
    file_connection_without_s3.close()

    assert RemotePath("some_dir") in file_connection_without_s3.listdir("/")


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_rename_file(file_all_connections, upload_test_files, path_type):
    with file_all_connections as connection:
        connection.rename_file(
            source_file_path=path_type("/export/news_parse/exclude_dir/file_5.txt"),
            target_file_path=path_type("/export/news_parse/exclude_dir/file_55.txt"),
        )

    list_dir = file_all_connections.listdir("/export/news_parse/exclude_dir/")

    assert RemotePath("file_55.txt") in list_dir
    assert RemotePath("file_5.txt") not in list_dir


def test_file_connection_read_text(file_all_connections, source_path, upload_files_with_encoding):
    read_text = file_all_connections.read_text(path=upload_files_with_encoding["utf"])

    assert isinstance(read_text, str)
    assert read_text == "тестовый текст в  тестовом файле\n"


def test_file_connection_read_bytes(file_all_connections, source_path, upload_files_with_encoding):
    read_bytes = file_all_connections.read_bytes(path=upload_files_with_encoding["ascii"])

    assert isinstance(read_bytes, bytes)
    assert read_bytes == b"test text in test file\n"


@pytest.mark.parametrize(
    "path,exception",
    [(lazy_fixture("source_path"), NotAFileError), ("/export/news_parse/no_such_file.txt", FileNotFoundError)],
)
def test_file_connection_read_text_negative(
    file_all_connections,
    source_path,
    upload_files_with_encoding,
    path,
    exception,
):
    with pytest.raises(exception):
        file_all_connections.read_text(path=path)


@pytest.mark.parametrize(
    "path,exception",
    [(lazy_fixture("source_path"), NotAFileError), ("/export/news_parse/no_such_file.txt", FileNotFoundError)],
)
def test_file_connection_read_bytes_negative(
    file_all_connections,
    source_path,
    upload_files_with_encoding,
    path,
    exception,
):
    with pytest.raises(exception):
        file_all_connections.read_bytes(path=path)


@pytest.mark.parametrize(
    "file_name",
    ["file_connection_write_text.txt", "file_connection_utf.txt"],
    ids=["new file", "file existed"],
)
def test_file_connection_write_text(file_all_connections, source_path, file_name, upload_files_with_encoding):
    file_all_connections.write_text(path=source_path / file_name, content="тестовый текст в  utf-8")

    assert file_all_connections.read_text(source_path / file_name) == "тестовый текст в  utf-8"


@pytest.mark.parametrize(
    "file_name",
    ["file_connection_write_bytes.txt", "file_connection_utf.txt"],
    ids=["new file", "file existed"],
)
def test_file_connection_write_bytes(file_all_connections, source_path, file_name, upload_files_with_encoding):
    file_all_connections.write_bytes(path=source_path / file_name, content=b"ascii test text")
    assert file_all_connections.read_bytes(source_path / file_name) == b"ascii test text"


def test_file_connection_write_text_fail_on_bytes_input(file_all_connections, source_path):
    with pytest.raises(TypeError):
        file_all_connections.write_text(path=source_path / "some_file_name.txt", content=b"bytes to text")


def test_file_connection_write_bytes_fail_on_text_input(file_all_connections, source_path):
    with pytest.raises(TypeError):
        file_all_connections.write_bytes(path=source_path / "some_file_name.txt", content="text to bytes")


def test_file_connection_write_encoding(file_all_connections, source_path):
    file_all_connections.write_text(
        path=source_path / "cp_1251_file",
        content="тестовый текст в  utf-8",
        encoding="cp1251",
    )

    assert file_all_connections.read_bytes(path=source_path / "cp_1251_file") == "тестовый текст в  utf-8".encode(
        "cp1251",
    )


def test_file_connection_read_encoding(file_all_connections, source_path):
    file_all_connections.write_bytes(
        path=source_path / "cp_1251_file",
        content="тестовый текст в  utf-8".encode("cp1251"),
    )

    assert (
        file_all_connections.read_text(path=source_path / "cp_1251_file", encoding="cp1251")
        == "тестовый текст в  utf-8"
    )


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_path_exists(file_all_connections, upload_test_files, path_type):
    assert file_all_connections.path_exists(path_type("/export/news_parse/exclude_dir/file_5.txt"))
    assert file_all_connections.path_exists(path_type("/export/news_parse/exclude_dir"))
    assert not file_all_connections.path_exists(path_type("/export/news_parse/path_not_exist"))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_is_dir(file_all_connections, upload_test_files, path_type):
    assert file_all_connections.is_dir(path_type("/export/news_parse/exclude_dir"))
    assert not file_all_connections.is_dir(path_type("/export/news_parse/exclude_dir/file_5.txt"))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_is_dir_negative(file_all_connections, upload_test_files, path_type):
    with pytest.raises(DirectoryNotFoundError):
        file_all_connections.is_dir(path_type("/export/news_parse/path_not_exist"))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_is_file(file_all_connections, upload_test_files, path_type):
    assert file_all_connections.is_file(path_type("/export/news_parse/exclude_dir/file_5.txt"))
    assert not file_all_connections.is_file(path_type("/export/news_parse/exclude_dir"))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_is_file_negative(file_all_connections, upload_test_files, path_type):
    with pytest.raises(FileNotFoundError):
        file_all_connections.is_file(path_type("/export/news_parse/path_not_exist"))
