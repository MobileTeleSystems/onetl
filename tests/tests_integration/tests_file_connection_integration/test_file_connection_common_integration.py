import os
import secrets
from pathlib import PurePosixPath

import pytest
from pytest_lazyfixture import lazy_fixture

from onetl.exception import DirectoryNotFoundError, NotAFileError
from onetl.impl import RemotePath


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_rm_dir_recursive(
    file_all_connections,
    source_path,
    path_type,
):
    file_all_connections.remove_dir(path_type(os.fspath(source_path)), recursive=True)

    assert not file_all_connections.path_exists(source_path)

    if file_all_connections.path_exists(source_path.parent):
        # S3 does not support creating directories
        parent_paths = [os.fspath(path) for path in file_all_connections.list_dir(source_path.parent)]
        assert source_path.name not in parent_paths


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_remove_dir_non_empty(file_all_connections, source_path, upload_test_files, path_type):
    with pytest.raises(Exception):
        file_all_connections.remove_dir(path_type(os.fspath(source_path)))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_remove_dir_fake_dir(file_all_connections, upload_test_files, path_type):
    # Does not raise Exception
    file_all_connections.remove_dir(path_type("/some/fake/dir"))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_create_dir(file_all_connections, source_path, path_type):
    path = source_path / "some_dir"

    file_all_connections.close()
    file_all_connections.create_dir(path_type(os.fspath(path)))
    file_all_connections.close()
    # `close` called twice is not an error
    file_all_connections.close()

    if file_all_connections.path_exists(path):
        # S3 does not support creating directories
        assert RemotePath("some_dir") in file_all_connections.list_dir(path.parent)


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_rename_file(file_all_connections, source_path, upload_test_files, path_type):
    with file_all_connections as connection:
        connection.rename_file(
            source_file_path=path_type(os.fspath(source_path / "exclude_dir/file_5.txt")),
            target_file_path=path_type(os.fspath(source_path / "exclude_dir/file_55.txt")),
        )

    list_dir = file_all_connections.list_dir(source_path / "exclude_dir/")

    assert RemotePath("file_55.txt") in list_dir
    assert RemotePath("file_5.txt") not in list_dir


def test_file_connection_read_text(file_all_connections, upload_files_with_encoding):
    read_text = file_all_connections.read_text(path=upload_files_with_encoding["utf"])

    assert isinstance(read_text, str)
    assert read_text == "тестовый текст в  тестовом файле\n"


def test_file_connection_read_bytes(file_all_connections, upload_files_with_encoding):
    read_bytes = file_all_connections.read_bytes(path=upload_files_with_encoding["ascii"])

    assert isinstance(read_bytes, bytes)
    assert read_bytes == b"test text in test file\n"


@pytest.mark.parametrize(
    "path,exception",
    [(lazy_fixture("source_path"), NotAFileError), ("/no_such_file.txt", FileNotFoundError)],
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
    [(lazy_fixture("source_path"), NotAFileError), ("/no_such_file.txt", FileNotFoundError)],
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
def test_file_connection_path_exists(file_all_connections, source_path, upload_test_files, path_type):
    assert file_all_connections.path_exists(path_type(os.fspath(source_path / "exclude_dir/file_5.txt")))
    assert file_all_connections.path_exists(path_type(os.fspath(source_path / "exclude_dir")))
    assert not file_all_connections.path_exists(path_type(os.fspath(source_path / "path_not_exist")))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_is_dir(file_all_connections, source_path, upload_test_files, path_type):
    assert file_all_connections.is_dir(path_type(os.fspath(source_path / "exclude_dir")))
    assert not file_all_connections.is_dir(path_type(os.fspath(source_path / "exclude_dir/file_5.txt")))

    with pytest.raises(DirectoryNotFoundError):
        file_all_connections.is_dir(path_type(os.fspath(source_path / "path_not_exist")))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_is_file(file_all_connections, source_path, upload_test_files, path_type):
    assert file_all_connections.is_file(path_type(os.fspath(source_path / "exclude_dir/file_5.txt")))
    assert not file_all_connections.is_file(path_type(os.fspath(source_path / "exclude_dir")))

    with pytest.raises(FileNotFoundError):
        file_all_connections.is_file(path_type(os.fspath(source_path / "path_not_exist")))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_download_file(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
    path_type,
):
    local_path = tmp_path_factory.mktemp("local_path")
    remote_file_path = source_path / "news_parse_zp/2018_03_05_10_00_00/newsage-zp-2018_03_05_10_00_00.csv"

    download_result = file_all_connections.download_file(
        remote_file_path=path_type(os.fspath(remote_file_path)),
        local_file_path=path_type(local_path / "file.csv"),
    )

    assert download_result.exists()
    assert download_result.stat().st_size == file_all_connections.resolve_file(remote_file_path).stat().st_size
    assert download_result.read_text() == file_all_connections.read_text(remote_file_path)


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_upload_file(file_all_connections, test_files, path_type):
    upload_result = file_all_connections.upload_file(
        local_file_path=path_type(test_files[0]),
        remote_file_path=path_type(path_type(f"/tmp/test_upload_{secrets.token_hex(5)}")),
    )

    assert upload_result.exists()
    assert upload_result.stat().st_size == test_files[0].stat().st_size
    assert file_all_connections.read_text(upload_result) == test_files[0].read_text()


@pytest.mark.parametrize(
    "path,exception",
    [
        ("exclude_dir/", NotAFileError),
        ("exclude_dir/file_not_exists", FileNotFoundError),
    ],
    ids=["directory", "file"],
)
def test_file_connection_download_file_wrong_source_type(
    file_all_connections,
    upload_test_files,
    tmp_path_factory,
    source_path,
    path,
    exception,
):
    local_path = tmp_path_factory.mktemp("local_path")
    with pytest.raises(exception):
        file_all_connections.download_file(
            remote_file_path=source_path / path,
            local_file_path=local_path / "file_5.txt",
        )


@pytest.mark.parametrize("replace", [True, False])
def test_file_connection_download_file_wrong_target_type(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
    replace,
):
    local_path = tmp_path_factory.mktemp("local_path")
    with pytest.raises(NotAFileError):
        file_all_connections.download_file(
            remote_file_path=source_path / "exclude_dir/file_5.txt",
            local_file_path=local_path,
            replace=replace,
        )


@pytest.mark.parametrize(
    "source,exception",
    [("news_parse_zp", NotAFileError), ("file_not_exist", FileNotFoundError)],
    ids=["directory", "file"],
)
def test_file_connection_upload_file_wrong_source_type(file_all_connections, resource_path, source, exception):
    with pytest.raises(exception):
        file_all_connections.upload_file(
            local_file_path=resource_path / source,
            remote_file_path=f"/tmp/test_upload_{secrets.token_hex(5)}",
        )


@pytest.mark.parametrize("replace", [True, False])
def test_file_connection_upload_file_wrong_target_type(
    file_all_connections,
    source_path,
    upload_test_files,
    test_files,
    replace,
):
    with pytest.raises(NotAFileError):
        file_all_connections.upload_file(
            local_file_path=test_files[0],
            remote_file_path=source_path / "exclude_dir",
            replace=replace,
        )


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_download_replace_target(
    file_all_connections,
    source_path,
    upload_files_with_encoding,
    tmp_path_factory,
    path_type,
):
    local_path = tmp_path_factory.mktemp("local_path")
    file_path = local_path / "file.txt"
    file_path.write_text("text to replace")
    remote_file_path = source_path / "file_connection_utf.txt"

    download_result = file_all_connections.download_file(
        remote_file_path=path_type(remote_file_path),
        local_file_path=path_type(file_path),
        replace=True,
    )

    assert download_result.exists()
    assert download_result.stat().st_size == file_all_connections.resolve_file(remote_file_path).stat().st_size
    assert download_result.read_text() == "тестовый текст в  тестовом файле\n"


def test_file_connection_download_replace_target_negative(
    file_all_connections,
    source_path,
    upload_files_with_encoding,
    tmp_path_factory,
):
    local_path = tmp_path_factory.mktemp("local_path")
    file_path = local_path / "file.txt"
    file_path.write_text("test file")
    remote_file_path = source_path / "file_connection_utf.txt"

    with pytest.raises(FileExistsError):
        file_all_connections.download_file(
            remote_file_path=remote_file_path,
            local_file_path=file_path,
            replace=False,
        )
    assert file_path.read_text() == "test file"


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_upload_replace_target(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
    path_type,
):
    local_path = tmp_path_factory.mktemp("local_path")
    file_path = local_path / "file.txt"
    file_path.write_text("test local file")

    upload_result = file_all_connections.upload_file(
        local_file_path=path_type(file_path),
        remote_file_path=path_type(os.fspath(source_path / "exclude_dir/file_5.txt")),
        replace=True,
    )

    assert upload_result.exists()
    assert upload_result.stat().st_size == file_path.stat().st_size
    assert file_all_connections.read_text(upload_result) == "test local file"


def test_file_connection_upload_replace_target_negative(
    file_all_connections,
    source_path,
    tmp_path_factory,
    upload_files_with_encoding,
    test_files,
):
    local_path = tmp_path_factory.mktemp("local_path")
    file_path = local_path / "file.txt"
    file_path.write_text("test local file")

    with pytest.raises(FileExistsError):
        file_all_connections.upload_file(
            local_file_path=file_path,
            remote_file_path=source_path / "file_connection_utf.txt",
            replace=False,
        )

    assert (
        file_all_connections.read_text(source_path / "file_connection_utf.txt") == "тестовый текст в  тестовом файле\n"
    )
