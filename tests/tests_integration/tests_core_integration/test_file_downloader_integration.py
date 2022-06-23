import logging
import secrets
import tempfile
from pathlib import Path, PurePosixPath

import pytest

from onetl.core import FileDownloader, FileFilter, FileSet
from onetl.exception import DirectoryNotFoundError, NotAFileError
from onetl.impl import (
    FailedRemoteFile,
    FileWriteMode,
    LocalPath,
    RemoteFile,
    RemotePath,
)


def test_downloader_view_file(file_connection, source_path, upload_test_files):
    downloader = FileDownloader(
        connection=file_connection,
        source_path=source_path,
        local_path="/some/path",
    )

    remote_files = downloader.view_files()
    remote_files_list = []

    for root, _dirs, files in file_connection.walk(source_path):
        for file in files:
            remote_files_list.append(RemotePath(root) / file)

    assert remote_files
    assert sorted(remote_files) == sorted(remote_files_list)


@pytest.mark.parametrize("path_type", [str, PurePosixPath], ids=["path_type str", "path_type PurePosixPath"])
@pytest.mark.parametrize(
    "run_path_type",
    [str, Path],
    ids=["run_path_type str", "run_path_type Path"],
)
def test_downloader_run(file_connection, source_path, upload_test_files, path_type, run_path_type, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_connection,
        source_path=path_type(source_path),
        local_path=run_path_type(local_path),
    )

    download_result = downloader.run()

    assert not download_result.failed
    assert not download_result.skipped
    assert not download_result.missing
    assert download_result.successful

    assert sorted(download_result.successful) == sorted(
        local_path / file.relative_to(source_path) for file in upload_test_files
    )

    for local_file in download_result.successful:
        assert isinstance(local_file, LocalPath)

        assert local_file.exists()
        assert local_file.is_file()
        assert not local_file.is_dir()

        remote_file_path = source_path / local_file.relative_to(local_path)
        remote_file = file_connection.get_file(remote_file_path)

        # file size is same as expected
        assert local_file.stat().st_size == file_connection.get_stat(remote_file).st_size

        # file content is same as expected
        assert local_file.read_bytes() == file_connection.read_bytes(remote_file)


def test_downloader_run_delete_source(
    file_connection,
    source_path,
    resource_path,
    upload_test_files,
    tmp_path_factory,
    caplog,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_connection,
        source_path=source_path,
        local_path=local_path,
        options=FileDownloader.Options(delete_source=True),
    )

    with caplog.at_level(logging.WARNING):
        download_result = downloader.run()

        assert "SOURCE FILES WILL BE PERMANENTLY DELETED AFTER DOWNLOADING !!!" in caplog.text

    assert not download_result.failed
    assert not download_result.skipped
    assert not download_result.missing
    assert download_result.successful

    assert sorted(download_result.successful) == sorted(
        local_path / file.relative_to(source_path) for file in upload_test_files
    )

    for local_file in download_result.successful:
        assert local_file.exists()
        assert local_file.is_file()
        assert not local_file.is_dir()

        # source_path contain a copy of files from resource_path
        # so check downloaded files content using them as a reference
        original_file = resource_path / local_file.relative_to(local_path)

        assert local_file.stat().st_size == original_file.stat().st_size
        assert local_file.read_bytes() == original_file.read_bytes()

    remote_files = FileSet()
    for root, _dirs, files in file_connection.walk(source_path):
        for file in files:
            remote_files.add(RemoteFile(path=root / file.name, stats=file.stats))

    assert not remote_files


@pytest.mark.parametrize("path_type", [str, Path])
def test_downloader_run_with_filter_exclude_dir(
    file_connection,
    source_path,
    upload_test_files,
    path_type,
    tmp_path_factory,
):
    local_path = tmp_path_factory.mktemp("local_path")
    exclude_dir = path_type("/export/news_parse/exclude_dir")

    downloader = FileDownloader(
        connection=file_connection,
        source_path=source_path,
        local_path=local_path,
        filter=FileFilter(exclude_dirs=[exclude_dir]),
    )

    download_result = downloader.run()

    assert not download_result.failed
    assert not download_result.skipped
    assert not download_result.missing
    assert download_result.successful

    assert sorted(download_result.successful) == sorted(
        local_path / file.relative_to(source_path)
        for file in upload_test_files
        if PurePosixPath(exclude_dir) not in file.parents
    )


def test_downloader_with_filter_glob(file_connection, source_path, upload_test_files, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")
    file_pattern = "*.csv"

    downloader = FileDownloader(
        connection=file_connection,
        source_path=source_path,
        local_path=local_path,
        filter=FileFilter(glob=file_pattern),
    )

    download_result = downloader.run()

    assert not download_result.failed
    assert not download_result.skipped
    assert not download_result.missing
    assert download_result.successful

    assert sorted(download_result.successful) == sorted(
        local_path / file.relative_to(source_path) for file in upload_test_files if file.match(file_pattern)
    )


@pytest.mark.parametrize(
    "source_path_value",
    [None, "/export/news_parse"],
    ids=["Without source_path", "With source path"],
)
def test_downloader_run_with_files_absolute(
    file_connection,
    source_path,
    upload_test_files,
    source_path_value,
    tmp_path_factory,
    caplog,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_connection,
        source_path=source_path_value,
        local_path=local_path,
    )

    with caplog.at_level(logging.WARNING):
        download_result = downloader.run(upload_test_files)

        if source_path_value:
            assert (
                "Passed both ``source_path`` and file collection at the same time. File collection will be used"
            ) in caplog.text

    assert not download_result.failed
    assert not download_result.skipped
    assert not download_result.missing
    assert download_result.successful

    if source_path_value:
        local_files = [local_path / file.relative_to(source_path) for file in upload_test_files]
    else:
        # no source path - do not preserve folder structure
        local_files = [local_path / file.name for file in upload_test_files]

    assert sorted(download_result.successful) == sorted(local_files)

    for remote_file_path in upload_test_files:
        if source_path_value:
            local_file = local_path / remote_file_path.relative_to(source_path)
        else:
            local_file = local_path / remote_file_path.name

        assert local_file.exists()
        assert local_file.is_file()
        assert not local_file.is_dir()

        remote_file = file_connection.get_file(remote_file_path)

        # file size is same as expected
        assert local_file.stat().st_size == file_connection.get_stat(remote_file).st_size
        assert remote_file.stat().st_size == file_connection.get_stat(remote_file).st_size

        # file content is same as expected
        assert local_file.read_bytes() == file_connection.read_bytes(remote_file)


def test_downloader_run_with_files_relative_and_source_path(
    file_connection,
    source_path,
    upload_test_files,
    tmp_path_factory,
):
    local_path = tmp_path_factory.mktemp("local_path")
    relative_files_path = [file.relative_to(source_path) for file in upload_test_files]

    downloader = FileDownloader(
        connection=file_connection,
        source_path=source_path,
        local_path=local_path,
    )

    download_result = downloader.run(file for file in relative_files_path)

    assert not download_result.failed
    assert not download_result.skipped
    assert not download_result.missing
    assert download_result.successful

    assert sorted(download_result.successful) == sorted(local_path / file for file in relative_files_path)

    for remote_file_path in upload_test_files:
        local_file = local_path / remote_file_path.relative_to(source_path)

        assert local_file.exists()
        assert local_file.is_file()
        assert not local_file.is_dir()

        remote_file = file_connection.get_file(remote_file_path)

        # file size is same as expected
        assert local_file.stat().st_size == file_connection.get_stat(remote_file).st_size
        assert remote_file.stat().st_size == file_connection.get_stat(remote_file).st_size

        # file content is same as expected
        assert local_file.read_bytes() == file_connection.read_bytes(remote_file)


def test_downloader_run_without_files_and_source_path(file_connection, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_connection,
        local_path=local_path,
    )
    with pytest.raises(ValueError, match="Neither file collection nor ``source_path`` are passed"):
        downloader.run()


@pytest.mark.parametrize(
    "pass_source_path",
    [False, True],
    ids=["Without source_path", "With source_path"],
)
def test_downloader_run_with_empty_files_input(request, file_connection, pass_source_path, tmp_path_factory):
    source_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    file_connection.mkdir(source_path)

    def finalizer():
        file_connection.rmdir(source_path, recursive=True)

    request.addfinalizer(finalizer)

    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_connection,
        local_path=local_path,
        source_path=source_path if pass_source_path else None,
    )

    download_result = downloader.run([])

    assert not download_result.failed
    assert not download_result.skipped
    assert not download_result.missing
    assert not download_result.successful


def test_downloader_run_with_empty_source_path(request, file_connection, tmp_path_factory):
    source_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    file_connection.mkdir(source_path)

    def finalizer():
        file_connection.rmdir(source_path, recursive=True)

    request.addfinalizer(finalizer)

    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_connection,
        local_path=local_path,
        source_path=source_path,
    )

    download_result = downloader.run()

    assert not download_result.failed
    assert not download_result.skipped
    assert not download_result.missing
    assert not download_result.successful


def test_downloader_run_relative_path_without_source_path(file_connection, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_connection,
        local_path=local_path,
    )

    with pytest.raises(ValueError, match="Cannot pass relative file path with empty ``source_path``"):
        downloader.run(["some/relative/path/file.txt"])


def test_downloader_run_absolute_path_not_match_source_path(
    file_connection,
    source_path,
    tmp_path_factory,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_connection,
        source_path=source_path,
        local_path=local_path,
    )

    error_message = f"File path '/some/relative/path/file.txt' does not match source_path '{source_path}'"
    with pytest.raises(ValueError, match=error_message):
        downloader.run(["/some/relative/path/file.txt"])


@pytest.mark.parametrize(
    "options",
    [{"mode": "error"}, FileDownloader.Options(mode="error"), FileDownloader.Options(mode=FileWriteMode.ERROR)],
)
def test_downloader_mode_error(file_connection, source_path, upload_test_files, options, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")

    # make copy of files to download in the local_path
    local_files = []
    local_files_stat = {}
    for test_file in upload_test_files:
        local_file = local_path / test_file.relative_to(source_path)

        local_file.parent.mkdir(parents=True, exist_ok=True)
        local_file.write_text("unchanged")
        local_files.append(local_file)
        local_files_stat[local_file] = local_file.stat()

    downloader = FileDownloader(
        connection=file_connection,
        source_path=source_path,
        local_path=local_path,
        options=options,
    )

    download_result = downloader.run()

    assert not download_result.successful
    assert not download_result.missing
    assert not download_result.skipped
    assert download_result.failed

    assert sorted(download_result.failed) == sorted(upload_test_files)

    for remote_file in download_result.failed:
        assert isinstance(remote_file, FailedRemoteFile)

        assert remote_file.exists()
        assert remote_file.is_file()
        assert not remote_file.is_dir()

        assert isinstance(remote_file.exception, FileExistsError)

        local_file = local_path / remote_file.relative_to(source_path)
        assert f"Local directory already contains file '{local_file}'" in str(remote_file.exception)

        # file size wasn't changed
        assert local_file.stat().st_size != remote_file.stat().st_size
        assert local_file.stat().st_size == local_files_stat[local_file].st_size

        # file content wasn't changed
        assert local_file.read_text() == "unchanged"


def test_downloader_mode_ignore(file_connection, source_path, upload_test_files, tmp_path_factory, caplog):
    local_path = tmp_path_factory.mktemp("local_path")

    # make copy of files to download in the local_path
    local_files = []
    local_files_stat = {}
    for test_file in upload_test_files:
        local_file = local_path / test_file.relative_to(source_path)

        local_file.parent.mkdir(parents=True, exist_ok=True)
        local_file.write_text("unchanged")
        local_files.append(local_file)
        local_files_stat[local_file] = local_file.stat()

    downloader = FileDownloader(
        connection=file_connection,
        source_path=source_path,
        local_path=local_path,
        options=FileDownloader.Options(mode=FileWriteMode.IGNORE),
    )

    with caplog.at_level(logging.WARNING):
        download_result = downloader.run()

        for file in local_files:
            assert f"Local directory already contains file '{file}', skipping" in caplog.text

    assert not download_result.successful
    assert not download_result.failed
    assert not download_result.missing
    assert download_result.skipped

    assert sorted(download_result.skipped) == sorted(upload_test_files)

    for remote_file in download_result.skipped:
        assert isinstance(remote_file, RemoteFile)

        assert remote_file.exists()
        assert remote_file.is_file()
        assert not remote_file.is_dir()

        local_file = local_path / remote_file.relative_to(source_path)

        # file size wasn't changed
        assert local_file.stat().st_size != remote_file.stat().st_size
        assert local_file.stat().st_size == local_files_stat[local_file].st_size

        # file content wasn't changed
        assert local_file.read_text() == "unchanged"


def test_downloader_mode_overwrite(file_connection, source_path, upload_test_files, tmp_path_factory, caplog):
    local_path = tmp_path_factory.mktemp("local_path")

    # make copy of files to download in the local_path
    local_files = []
    local_files_stat = {}
    for test_file in upload_test_files:
        local_file = local_path / test_file.relative_to(source_path)

        local_file.parent.mkdir(parents=True, exist_ok=True)
        local_file.write_text("unchanged")
        local_files.append(local_file)
        local_files_stat[local_file] = local_file.stat()

    downloader = FileDownloader(
        connection=file_connection,
        source_path=source_path,
        local_path=local_path,
        options=FileDownloader.Options(mode=FileWriteMode.OVERWRITE),
    )

    with caplog.at_level(logging.WARNING):
        download_result = downloader.run()

        for changed_file in local_files:
            assert f"Local directory already contains file '{changed_file}', overwriting" in caplog.text

    assert not download_result.failed
    assert not download_result.missing
    assert not download_result.skipped
    assert download_result.successful

    assert sorted(download_result.successful) == sorted(
        local_path / file.relative_to(source_path) for file in upload_test_files
    )

    for remote_file_path in upload_test_files:
        local_file = local_path / remote_file_path.relative_to(source_path)

        assert local_file.exists()
        assert local_file.is_file()
        assert not local_file.is_dir()

        remote_file = file_connection.get_file(remote_file_path)

        # file size was changed
        assert local_file.stat().st_size != local_files_stat[local_file].st_size
        assert local_file.stat().st_size == file_connection.get_stat(remote_file).st_size
        assert remote_file.stat().st_size == file_connection.get_stat(remote_file).st_size

        # file content was changed
        assert local_file.read_text() != "unchanged"
        assert local_file.read_bytes() == file_connection.read_bytes(remote_file)


def test_downloader_mode_delete_all(file_connection, source_path, upload_test_files, tmp_path_factory, caplog):
    local_path = tmp_path_factory.mktemp("local_path")

    new_local_file = local_path / secrets.token_hex(5)
    new_local_file.touch()

    downloader = FileDownloader(
        connection=file_connection,
        source_path=source_path,
        local_path=local_path,
        options=FileDownloader.Options(mode=FileWriteMode.DELETE_ALL),
    )

    with caplog.at_level(logging.WARNING):
        download_result = downloader.run()
        assert "LOCAL DIRECTORY WILL BE CLEANED UP BEFORE DOWNLOADING FILES !!!" in caplog.text

    assert not download_result.failed
    assert not download_result.missing
    assert not download_result.skipped
    assert download_result.successful

    assert sorted(download_result.successful) == sorted(
        local_path / file.relative_to(source_path) for file in upload_test_files
    )

    assert not new_local_file.exists()


def test_downloader_run_missing_file(request, file_connection, upload_test_files, tmp_path_factory, caplog):
    local_path = tmp_path_factory.mktemp("local_path")
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    file_connection.mkdir(target_path)

    def finalizer():
        file_connection.rmdir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    # upload files
    downloader = FileDownloader(
        connection=file_connection,
        local_path=local_path,
    )

    missing_file = target_path / "missing"

    with caplog.at_level(logging.WARNING):
        download_result = downloader.run(upload_test_files + [missing_file])

        assert f"Missing file '{missing_file}', skipping" in caplog.text

    assert not download_result.failed
    assert not download_result.skipped
    assert download_result.missing
    assert download_result.successful

    assert len(download_result.successful) == len(upload_test_files)
    assert len(download_result.missing) == 1

    assert download_result.missing == {missing_file}
    assert isinstance(download_result.missing[0], RemotePath)


def test_downloader_source_path_does_not_exist(file_connection, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")
    source_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    downloader = FileDownloader(
        connection=file_connection,
        source_path=source_path,
        local_path=local_path,
    )

    with pytest.raises(DirectoryNotFoundError, match=f"'{source_path}' does not exist"):
        downloader.run()


def test_downloader_source_path_not_a_directory(request, file_connection, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")

    source_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    file_connection.write_text(source_path, "abc")

    def finalizer():
        file_connection.remove_file(source_path)

    request.addfinalizer(finalizer)

    downloader = FileDownloader(
        connection=file_connection,
        source_path=source_path,
        local_path=local_path,
    )

    with pytest.raises(NotADirectoryError, match=f"'{source_path}' is not a directory"):
        downloader.run()

    file_connection.remove_file(source_path)


def test_downloader_local_path_not_a_directory(request, file_connection):
    source_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    file_connection.mkdir(source_path)

    def finalizer():
        file_connection.rmdir(source_path)

    request.addfinalizer(finalizer)

    with tempfile.NamedTemporaryFile() as file:
        downloader = FileDownloader(
            connection=file_connection,
            source_path=source_path,
            local_path=file.name,
        )

        with pytest.raises(NotADirectoryError, match=f"'{file.name}' is not a directory"):
            downloader.run()


def test_downloader_run_input_is_not_file(request, file_connection, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    not_a_file = target_path / "not_a_file"

    file_connection.mkdir(not_a_file)

    def finalizer():
        file_connection.rmdir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    # upload files
    downloader = FileDownloader(
        connection=file_connection,
        local_path=local_path,
    )

    with pytest.raises(NotAFileError, match=f"'{not_a_file}' is not a file"):
        downloader.run([not_a_file])
