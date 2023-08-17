import os
import secrets
from pathlib import PurePosixPath

import pytest

from onetl.base import SupportsRenameDir
from onetl.exception import DirectoryExistsError, DirectoryNotFoundError, NotAFileError
from onetl.impl import RemotePath


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_rm_dir_recursive(
    file_connection_with_path_and_files,
    path_type,
):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    file_connection.remove_dir(path_type(os.fspath(remote_path)), recursive=True)

    assert not file_connection.path_exists(remote_path)

    if file_connection.path_exists(remote_path.parent):
        # S3 does not support creating directories
        parent_paths = [os.fspath(path) for path in file_connection.list_dir(remote_path.parent)]
        assert remote_path.name not in parent_paths


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_remove_dir_non_empty(file_connection_with_path_and_files, path_type):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    with pytest.raises(Exception):
        file_connection.remove_dir(path_type(os.fspath(remote_path)))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_remove_dir_fake_dir(file_connection, path_type):
    # Does not raise Exception
    file_connection.remove_dir(path_type("/some/fake/dir"))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_create_dir(file_connection_with_path, path_type):
    file_connection, remote_path = file_connection_with_path
    path = remote_path / "some_dir"

    file_connection.close()
    file_connection.create_dir(path_type(os.fspath(path)))
    file_connection.close()
    # `close` called twice is not an error
    file_connection.close()

    if file_connection.path_exists(path):
        # S3 does not support creating directories
        assert RemotePath("some_dir") in file_connection.list_dir(path.parent)


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_rename_file(file_connection_with_path_and_files, path_type):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    with file_connection as connection:
        connection.rename_file(
            source_file_path=path_type(os.fspath(remote_path / "ascii.txt")),
            target_file_path=path_type(os.fspath(remote_path / "new.txt")),
        )

    list_dir = file_connection.list_dir(remote_path)

    assert RemotePath("new.txt") in list_dir
    assert RemotePath("ascii.txt") not in list_dir


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_rename_dir(file_connection_with_path_and_files, path_type):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    if not isinstance(file_connection, SupportsRenameDir):
        # S3 does not have directories
        return

    def stringify(items):
        return list(map(os.fspath, items))

    old_dir = remote_path / "exclude_dir"
    new_dir = remote_path / "exclude_dir1"
    files_before = list(file_connection.walk(old_dir))

    file_connection.rename_dir(
        source_dir_path=path_type(os.fspath(old_dir)),
        target_dir_path=path_type(os.fspath(new_dir)),
    )

    list_dir = file_connection.list_dir(remote_path)
    assert RemotePath("exclude_dir") not in list_dir
    assert RemotePath("exclude_dir1") in list_dir

    # root has different name, but all directories content is the same
    files_after = [
        (os.fspath(root), stringify(dirs), stringify(files)) for root, dirs, files in file_connection.walk(new_dir)
    ]
    assert files_after == [
        (os.fspath(new_dir / root.relative_to(old_dir)), stringify(dirs), stringify(files))
        for root, dirs, files in files_before
    ]


def test_file_connection_rename_dir_already_exists(request, file_connection_with_path_and_files):
    file_connection, remote_path, upload_files = file_connection_with_path_and_files
    if not isinstance(file_connection, SupportsRenameDir):
        # S3 does not have directories
        return

    old_dir = remote_path / "exclude_dir"
    new_dir = remote_path / "exclude_dir1"

    def finalizer():
        file_connection.remove_dir(new_dir)

    request.addfinalizer(finalizer)

    file_connection.create_dir(new_dir)

    with pytest.raises(DirectoryExistsError):
        file_connection.rename_dir(
            source_dir_path=old_dir,
            target_dir_path=new_dir,
        )


def test_file_connection_rename_dir_replace(request, file_connection_with_path_and_files):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    if not isinstance(file_connection, SupportsRenameDir):
        # S3 does not have directories
        return

    def stringify(items):
        return list(map(os.fspath, items))

    old_dir = remote_path / "exclude_dir"
    new_dir = remote_path / "exclude_dir1"

    def finalizer():
        file_connection.remove_dir(new_dir, recursive=True)

    request.addfinalizer(finalizer)

    file_connection.create_dir(new_dir)

    files_before = list(file_connection.walk(old_dir))

    file_connection.rename_dir(
        source_dir_path=old_dir,
        target_dir_path=new_dir,
        replace=True,
    )

    list_dir = file_connection.list_dir(remote_path)
    assert RemotePath("exclude_dir") not in list_dir
    assert RemotePath("exclude_dir1") in list_dir

    # root has different name, but all directories content is the same
    files_after = [
        (os.fspath(root), stringify(dirs), stringify(files)) for root, dirs, files in file_connection.walk(new_dir)
    ]
    assert files_after == [
        (os.fspath(new_dir / root.relative_to(old_dir)), stringify(dirs), stringify(files))
        for root, dirs, files in files_before
    ]


def test_file_connection_read_text(file_connection_with_path_and_files):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    content = file_connection.read_text(path=remote_path / "utf-8.txt")

    assert isinstance(content, str)
    assert content == "тестовый текст в  тестовом файле\n"


def test_file_connection_read_bytes(file_connection_with_path_and_files):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    content = file_connection.read_bytes(path=remote_path / "ascii.txt")

    assert isinstance(content, bytes)
    assert content == b"test text in test file\n"


@pytest.mark.parametrize(
    "pass_real_path, exception",
    [(True, NotAFileError), (False, FileNotFoundError)],
)
def test_file_connection_read_text_negative(
    file_connection_with_path_and_files,
    pass_real_path,
    exception,
):
    # uploading files only because S3 does not support empty directories
    file_connection, remote_path, _ = file_connection_with_path_and_files
    fake_path = "/missing.txt"
    with pytest.raises(exception):
        file_connection.read_text(path=remote_path if pass_real_path else fake_path)


@pytest.mark.parametrize(
    "pass_real_path, exception",
    [(True, NotAFileError), (False, FileNotFoundError)],
)
def test_file_connection_read_bytes_negative(
    file_connection_with_path_and_files,
    pass_real_path,
    exception,
):
    # uploading files only because S3 does not support empty directories
    file_connection, remote_path, _ = file_connection_with_path_and_files
    fake_path = "/missing.txt"
    with pytest.raises(exception):
        file_connection.read_bytes(path=remote_path if pass_real_path else fake_path)


@pytest.mark.parametrize(
    "file_name",
    ["new.txt", "utf-8.txt"],
    ids=["new file", "existing file"],
)
def test_file_connection_write_text(file_connection_with_path_and_files, file_name):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    file_connection.write_text(path=remote_path / file_name, content="тестовый текст в  utf-8")
    assert file_connection.read_text(remote_path / file_name) == "тестовый текст в  utf-8"


@pytest.mark.parametrize(
    "file_name",
    ["new.txt", "utf-8.txt"],
    ids=["new file", "existing file"],
)
def test_file_connection_write_bytes(file_connection_with_path_and_files, file_name):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    file_connection.write_bytes(path=remote_path / file_name, content=b"ascii test text")
    assert file_connection.read_bytes(remote_path / file_name) == b"ascii test text"


def test_file_connection_write_text_fail_on_bytes_input(file_connection_with_path):
    file_connection, remote_path = file_connection_with_path
    with pytest.raises(TypeError):
        file_connection.write_text(path=remote_path / "new.txt", content=b"bytes to text")


def test_file_connection_write_bytes_fail_on_text_input(file_connection_with_path):
    file_connection, remote_path = file_connection_with_path
    with pytest.raises(TypeError):
        file_connection.write_bytes(path=remote_path / "new.txt", content="text to bytes")


def test_file_connection_write_encoding(file_connection_with_path):
    file_connection, remote_path = file_connection_with_path
    file_connection.write_text(
        path=remote_path / "cp-1251.txt",
        content="тестовый текст в  utf-8",
        encoding="cp1251",
    )

    assert file_connection.read_bytes(path=remote_path / "cp-1251.txt") == "тестовый текст в  utf-8".encode(
        "cp1251",
    )


def test_file_connection_read_encoding(file_connection_with_path):
    file_connection, remote_path = file_connection_with_path
    file_connection.write_bytes(
        path=remote_path / "cp_1251_file",
        content="тестовый текст в  utf-8".encode("cp1251"),
    )

    assert file_connection.read_text(path=remote_path / "cp_1251_file", encoding="cp1251") == "тестовый текст в  utf-8"


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_path_exists(file_connection_with_path_and_files, path_type):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    assert file_connection.path_exists(path_type(os.fspath(remote_path / "ascii.txt")))
    assert file_connection.path_exists(path_type(os.fspath(remote_path)))
    assert not file_connection.path_exists(path_type(os.fspath(remote_path / "path_not_exist")))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_is_dir(file_connection_with_path_and_files, path_type):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    assert file_connection.is_dir(path_type(os.fspath(remote_path)))
    assert not file_connection.is_dir(path_type(os.fspath(remote_path / "ascii.txt")))

    with pytest.raises(DirectoryNotFoundError):
        file_connection.is_dir(path_type(os.fspath(remote_path / "path_not_exist")))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_is_file(file_connection_with_path_and_files, path_type):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    assert file_connection.is_file(path_type(os.fspath(remote_path / "ascii.txt")))
    assert not file_connection.is_file(path_type(os.fspath(remote_path)))

    with pytest.raises(FileNotFoundError):
        file_connection.is_file(path_type(os.fspath(remote_path / "path_not_exist")))


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_download_file(
    file_connection_with_path_and_files,
    tmp_path_factory,
    path_type,
):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    local_path = tmp_path_factory.mktemp("local_path")
    remote_file_path = remote_path / "some.csv"

    download_result = file_connection.download_file(
        remote_file_path=path_type(os.fspath(remote_file_path)),
        local_file_path=path_type(local_path / "file.csv"),
    )

    assert download_result.exists()
    assert download_result.stat().st_size == file_connection.resolve_file(remote_file_path).stat().st_size
    assert download_result.read_text() == file_connection.read_text(remote_file_path)


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_upload_file(file_connection, file_connection_test_files, path_type):
    test_files = file_connection_test_files
    upload_result = file_connection.upload_file(
        local_file_path=path_type(test_files[0]),
        remote_file_path=path_type(path_type(f"/tmp/test_file_{secrets.token_hex(5)}")),
    )

    assert upload_result.exists()
    assert upload_result.stat().st_size == test_files[0].stat().st_size
    assert file_connection.read_text(upload_result) == test_files[0].read_text()


@pytest.mark.parametrize(
    "path,exception",
    [
        ("exclude_dir/", NotAFileError),
        ("exclude_dir/file_not_exists", FileNotFoundError),
    ],
    ids=["directory", "file"],
)
def test_file_connection_download_file_wrong_source_type(
    file_connection_with_path_and_files,
    tmp_path_factory,
    path,
    exception,
):
    # uploading files only because S3 does not support empty directories
    file_connection, remote_path, _ = file_connection_with_path_and_files
    local_path = tmp_path_factory.mktemp("local_path")
    with pytest.raises(exception):
        file_connection.download_file(
            remote_file_path=remote_path / path,
            local_file_path=local_path / "file.txt",
        )


@pytest.mark.parametrize("replace", [True, False])
def test_file_connection_download_file_wrong_target_type(
    file_connection_with_path_and_files,
    tmp_path_factory,
    replace,
):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    local_path = tmp_path_factory.mktemp("local_path")
    with pytest.raises(NotAFileError):
        file_connection.download_file(
            remote_file_path=remote_path / "ascii.txt",
            local_file_path=local_path,
            replace=replace,
        )


@pytest.mark.parametrize(
    "source,exception",
    [("exclude_dir", NotAFileError), ("missing", FileNotFoundError)],
    ids=["directory", "missing"],
)
def test_file_connection_upload_file_wrong_source(file_connection, file_connection_resource_path, source, exception):
    resource_path = file_connection_resource_path
    with pytest.raises(exception):
        file_connection.upload_file(
            local_file_path=resource_path / source,
            remote_file_path=f"/tmp/test_file_{secrets.token_hex(5)}",
        )


@pytest.mark.parametrize("replace", [True, False])
def test_file_connection_upload_file_wrong_target_type(
    file_connection_with_path_and_files,
    file_connection_test_files,
    replace,
):
    # uploading files only because S3 does not support empty directories
    file_connection, remote_path, _ = file_connection_with_path_and_files
    test_files = file_connection_test_files
    with pytest.raises(NotAFileError):
        file_connection.upload_file(
            local_file_path=test_files[0],
            remote_file_path=remote_path / "exclude_dir",
            replace=replace,
        )


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_download_replace_target(
    file_connection_with_path_and_files,
    tmp_path_factory,
    path_type,
):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    local_path = tmp_path_factory.mktemp("local_path")
    file_path = local_path / "file.txt"
    file_path.write_text("text to replace")
    remote_file_path = remote_path / "utf-8.txt"

    download_result = file_connection.download_file(
        remote_file_path=path_type(remote_file_path),
        local_file_path=path_type(file_path),
        replace=True,
    )

    assert download_result.exists()
    assert download_result.stat().st_size == file_connection.resolve_file(remote_file_path).stat().st_size
    assert download_result.read_text() == "тестовый текст в  тестовом файле\n"


def test_file_connection_download_replace_target_negative(
    file_connection_with_path_and_files,
    tmp_path_factory,
):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    local_path = tmp_path_factory.mktemp("local_path")
    file_path = local_path / "file.txt"
    file_path.write_text("test file")
    remote_file_path = remote_path / "utf-8.txt"

    with pytest.raises(FileExistsError):
        file_connection.download_file(
            remote_file_path=remote_file_path,
            local_file_path=file_path,
            replace=False,
        )
    assert file_path.read_text() == "test file"


@pytest.mark.parametrize("path_type", [str, PurePosixPath])
def test_file_connection_upload_replace_target(
    file_connection_with_path_and_files,
    tmp_path_factory,
    path_type,
):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    local_path = tmp_path_factory.mktemp("local_path")
    file_path = local_path / "file.txt"
    file_path.write_text("test local file")

    upload_result = file_connection.upload_file(
        local_file_path=path_type(file_path),
        remote_file_path=path_type(os.fspath(remote_path / "new.txt")),
        replace=True,
    )

    assert upload_result.exists()
    assert upload_result.stat().st_size == file_path.stat().st_size
    assert file_connection.read_text(upload_result) == "test local file"


def test_file_connection_upload_replace_target_negative(
    file_connection_with_path_and_files,
    tmp_path_factory,
):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    local_path = tmp_path_factory.mktemp("local_path")
    file_path = local_path / "file.txt"
    file_path.write_text("test local file")

    with pytest.raises(FileExistsError):
        file_connection.upload_file(
            local_file_path=file_path,
            remote_file_path=remote_path / "utf-8.txt",
            replace=False,
        )

    assert file_connection.read_text(remote_path / "utf-8.txt") == "тестовый текст в  тестовом файле\n"
