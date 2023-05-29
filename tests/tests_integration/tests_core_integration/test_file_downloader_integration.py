import logging
import os
import re
import secrets
import tempfile
from datetime import timedelta
from pathlib import Path, PurePosixPath

import pytest
from etl_entities import FileListHWM
from pytest_lazyfixture import lazy_fixture

from onetl.exception import DirectoryNotFoundError, NotAFileError
from onetl.file import FileDownloader
from onetl.file.file_set import FileSet
from onetl.file.filter import ExcludeDir, Glob
from onetl.file.limit import MaxFilesCount
from onetl.impl import (
    FailedRemoteFile,
    FileWriteMode,
    LocalPath,
    RemoteFile,
    RemotePath,
)
from onetl.strategy import (
    IncrementalBatchStrategy,
    IncrementalStrategy,
    SnapshotBatchStrategy,
)


def test_downloader_view_file(file_all_connections, source_path, upload_test_files):
    downloader = FileDownloader(
        connection=file_all_connections,
        source_path=source_path,
        local_path="/some/path",
    )

    remote_files = downloader.view_files()
    remote_files_list = []

    for root, _dirs, files in file_all_connections.walk(source_path):
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
def test_downloader_run(
    file_all_connections,
    source_path,
    upload_test_files,
    path_type,
    run_path_type,
    tmp_path_factory,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
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
        remote_file = file_all_connections.resolve_file(remote_file_path)

        # file size is same as expected
        assert local_file.stat().st_size == file_all_connections.get_stat(remote_file).st_size

        # file content is same as expected
        assert local_file.read_bytes() == file_all_connections.read_bytes(remote_file)


def test_downloader_run_delete_source(
    file_all_connections,
    source_path,
    upload_test_files,
    resource_path,
    tmp_path_factory,
    caplog,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
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

    if not file_all_connections.path_exists(source_path):
        # S3 does not support creating directories
        return

    remote_files = FileSet()
    for root, _dirs, files in file_all_connections.walk(source_path):
        for file in files:
            remote_files.add(RemoteFile(path=root / file.name, stats=file.stats))

    assert not remote_files


@pytest.mark.parametrize("path_type", [str, Path])
def test_downloader_file_filter_exclude_dir(
    file_all_connections,
    source_path,
    upload_test_files,
    path_type,
    tmp_path_factory,
    caplog,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        source_path=source_path,
        local_path=local_path,
        filters=[ExcludeDir(path_type(source_path / "exclude_dir"))],
    )

    excluded = [
        source_path / "exclude_dir/file_4.txt",
        source_path / "exclude_dir/file_5.txt",
    ]

    with caplog.at_level(logging.INFO):
        download_result = downloader.run()
        assert "    filters = [" in caplog.text
        assert f"        ExcludeDir('{source_path}/exclude_dir')," in caplog.text
        assert "    ]" in caplog.text

    assert not download_result.failed
    assert not download_result.skipped
    assert not download_result.missing
    assert download_result.successful

    assert sorted(download_result.successful) == sorted(
        local_path / file.relative_to(source_path) for file in upload_test_files if file not in excluded
    )


def test_downloader_file_filter_glob(file_all_connections, source_path, upload_test_files, tmp_path_factory, caplog):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        source_path=source_path,
        local_path=local_path,
        filters=[Glob("*.csv")],
    )

    excluded = [
        source_path / "exclude_dir/file_4.txt",
        source_path / "exclude_dir/file_5.txt",
        source_path / "news_parse_zp/exclude_dir/file_1.txt",
        source_path / "news_parse_zp/exclude_dir/file_2.txt",
        source_path / "news_parse_zp/exclude_dir/file_3.txt",
    ]

    with caplog.at_level(logging.INFO):
        download_result = downloader.run()
        assert "    filters = [" in caplog.text
        assert "        Glob('*.csv')," in caplog.text
        assert "    ]" in caplog.text

    assert not download_result.failed
    assert not download_result.skipped
    assert not download_result.missing
    assert download_result.successful

    assert sorted(download_result.successful) == sorted(
        local_path / file.relative_to(source_path) for file in upload_test_files if file not in excluded
    )


def test_downloader_file_filter_is_ignored_by_user_input(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        source_path=source_path,
        local_path=local_path,
        filters=[Glob("*.csv")],
    )

    download_result = downloader.run(upload_test_files)

    # filter is not being applied to explicit files list
    assert sorted(download_result.successful) == sorted(
        local_path / file.relative_to(source_path) for file in upload_test_files
    )


@pytest.mark.parametrize(
    "source_path_value",
    [None, lazy_fixture("source_path")],
    ids=["Without source_path", "With source path"],
)
def test_downloader_run_with_files_absolute(
    file_all_connections,
    source_path,
    upload_test_files,
    source_path_value,
    tmp_path_factory,
    caplog,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        source_path=source_path_value,
        local_path=local_path,
    )

    with caplog.at_level(logging.WARNING):
        download_result = downloader.run(upload_test_files)

        if source_path_value:
            assert (
                "Passed both `source_path` and files list at the same time. Using explicit files list"
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

        remote_file = file_all_connections.resolve_file(remote_file_path)

        # file size is same as expected
        assert local_file.stat().st_size == file_all_connections.get_stat(remote_file).st_size
        assert remote_file.stat().st_size == file_all_connections.get_stat(remote_file).st_size

        # file content is same as expected
        assert local_file.read_bytes() == file_all_connections.read_bytes(remote_file)


def test_downloader_run_with_files_relative_and_source_path(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
):
    local_path = tmp_path_factory.mktemp("local_path")
    relative_files_path = [file.relative_to(source_path) for file in upload_test_files]

    downloader = FileDownloader(
        connection=file_all_connections,
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

        remote_file = file_all_connections.resolve_file(remote_file_path)

        # file size is same as expected
        assert local_file.stat().st_size == file_all_connections.get_stat(remote_file).st_size
        assert remote_file.stat().st_size == file_all_connections.get_stat(remote_file).st_size

        # file content is same as expected
        assert local_file.read_bytes() == file_all_connections.read_bytes(remote_file)


def test_downloader_run_without_files_and_source_path(file_all_connections, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        local_path=local_path,
    )
    with pytest.raises(ValueError, match="Neither file list nor `source_path` are passed"):
        downloader.run()


@pytest.mark.parametrize(
    "pass_source_path",
    [False, True],
    ids=["Without source_path", "With source_path"],
)
def test_downloader_run_with_empty_files_input(
    file_all_connections,
    pass_source_path,
    tmp_path_factory,
    upload_test_files,
    source_path,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        local_path=local_path,
        source_path=source_path if pass_source_path else None,
    )

    download_result = downloader.run([])  # this argument takes precedence

    assert not download_result.failed
    assert not download_result.skipped
    assert not download_result.missing
    assert not download_result.successful


def test_downloader_run_with_empty_source_path(request, file_all_connections, tmp_path_factory):
    source_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    file_all_connections.create_dir(source_path)
    if file_all_connections.path_exists(source_path):
        # S3 does not support creating directories

        def finalizer():
            file_all_connections.remove_dir(source_path, recursive=True)

        request.addfinalizer(finalizer)

        local_path = tmp_path_factory.mktemp("local_path")

        downloader = FileDownloader(
            connection=file_all_connections,
            local_path=local_path,
            source_path=source_path,
        )

        download_result = downloader.run()

        assert not download_result.failed
        assert not download_result.skipped
        assert not download_result.missing
        assert not download_result.successful


def test_downloader_run_relative_path_without_source_path(file_all_connections, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        local_path=local_path,
    )

    with pytest.raises(ValueError, match="Cannot pass relative file path with empty `source_path`"):
        downloader.run(["some/relative/path/file.txt"])


def test_downloader_run_absolute_path_not_match_source_path(
    file_all_connections,
    source_path,
    tmp_path_factory,
    upload_test_files,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
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
def test_downloader_mode_error(file_all_connections, source_path, upload_test_files, options, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")

    # make copy of files to download in the local_path
    local_files_stat = {}
    for test_file in upload_test_files:
        local_file = local_path / test_file.relative_to(source_path)

        local_file.parent.mkdir(parents=True, exist_ok=True)
        local_file.write_text("unchanged")
        local_files_stat[local_file] = local_file.stat()

    downloader = FileDownloader(
        connection=file_all_connections,
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
        assert re.search(rf"File '{local_file}' \(kind='file', .*\) already exists", str(remote_file.exception))

        # file size wasn't changed
        assert local_file.stat().st_size != remote_file.stat().st_size
        assert local_file.stat().st_size == local_files_stat[local_file].st_size

        # file content wasn't changed
        assert local_file.read_text() == "unchanged"


def test_downloader_mode_ignore(file_all_connections, source_path, upload_test_files, tmp_path_factory, caplog):
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
        connection=file_all_connections,
        source_path=source_path,
        local_path=local_path,
        options=FileDownloader.Options(mode=FileWriteMode.IGNORE),
    )

    with caplog.at_level(logging.WARNING):
        download_result = downloader.run()

        for file in local_files:
            assert re.search(rf"File '{file}' \(kind='file', .*\) already exists, skipping", caplog.text)

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


def test_downloader_mode_overwrite(file_all_connections, source_path, upload_test_files, tmp_path_factory, caplog):
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
        connection=file_all_connections,
        source_path=source_path,
        local_path=local_path,
        options=FileDownloader.Options(mode=FileWriteMode.OVERWRITE),
    )

    with caplog.at_level(logging.WARNING):
        download_result = downloader.run()

        for changed_file in local_files:
            assert re.search(rf"File '{changed_file}' \(kind='file', .*\) already exists, overwriting", caplog.text)

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

        remote_file = file_all_connections.resolve_file(remote_file_path)

        # file size was changed
        assert local_file.stat().st_size != local_files_stat[local_file].st_size
        assert local_file.stat().st_size == file_all_connections.get_stat(remote_file).st_size
        assert remote_file.stat().st_size == file_all_connections.get_stat(remote_file).st_size

        # file content was changed
        assert local_file.read_text() != "unchanged"
        assert local_file.read_bytes() == file_all_connections.read_bytes(remote_file)


@pytest.mark.parametrize("local_dir_exist", [True, False])
def test_downloader_mode_delete_all(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
    local_dir_exist,
    caplog,
):
    if local_dir_exist:
        local_path = tmp_path_factory.mktemp("local_path")
    else:
        local_path = Path(tempfile.gettempdir()) / secrets.token_hex()

    temp_file = local_path / secrets.token_hex(5)
    if local_dir_exist:
        temp_file.touch()

    downloader = FileDownloader(
        connection=file_all_connections,
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

    # folder contains only downloaded files
    assert sorted(item for item in local_path.glob("**/*") if item.is_file()) == sorted(download_result.successful)
    assert not temp_file.exists()


def test_downloader_run_missing_file(request, file_all_connections, upload_test_files, tmp_path_factory, caplog):
    local_path = tmp_path_factory.mktemp("local_path")
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    file_all_connections.create_dir(target_path)
    if file_all_connections.path_exists(target_path):
        # S3 does not support creating directories

        def finalizer():
            file_all_connections.remove_dir(target_path, recursive=True)

        request.addfinalizer(finalizer)

    # upload files
    downloader = FileDownloader(
        connection=file_all_connections,
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


def test_downloader_source_path_does_not_exist(file_all_connections, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")
    source_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    downloader = FileDownloader(
        connection=file_all_connections,
        source_path=source_path,
        local_path=local_path,
    )

    with pytest.raises(DirectoryNotFoundError, match=f"'{source_path}' does not exist"):
        downloader.run()


def test_downloader_source_path_not_a_directory(request, file_all_connections, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")

    source_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    file_all_connections.write_text(source_path, "abc")

    def finalizer():
        file_all_connections.remove_file(source_path)

    request.addfinalizer(finalizer)

    downloader = FileDownloader(
        connection=file_all_connections,
        source_path=source_path,
        local_path=local_path,
    )

    with pytest.raises(NotADirectoryError, match=rf"'{source_path}' \(kind='file', .*\) is not a directory"):
        downloader.run()


def test_downloader_local_path_not_a_directory(request, file_all_connections):
    source_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    file_all_connections.create_dir(source_path)

    def finalizer():
        file_all_connections.remove_dir(source_path)

    request.addfinalizer(finalizer)

    with tempfile.NamedTemporaryFile() as file:
        downloader = FileDownloader(
            connection=file_all_connections,
            source_path=source_path,
            local_path=file.name,
        )

        with pytest.raises(NotADirectoryError, match=rf"'{file.name}' \(kind='file', .*\) is not a directory"):
            downloader.run()


def test_downloader_run_input_is_not_file(request, file_all_connections, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")

    source_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    not_a_file = source_path / "not_a_file"

    file_all_connections.create_dir(not_a_file)

    if not file_all_connections.path_exists(not_a_file):
        # S3 does not support creating directories
        return

    def finalizer():
        file_all_connections.remove_dir(source_path, recursive=True)

    request.addfinalizer(finalizer)

    downloader = FileDownloader(
        connection=file_all_connections,
        local_path=local_path,
    )

    with pytest.raises(NotAFileError, match=rf"'{not_a_file}' \(kind='directory', .*\) is not a file"):
        downloader.run([not_a_file])


def test_downloader_with_file_limit(file_all_connections, source_path, upload_test_files, tmp_path_factory, caplog):
    limit = 2
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        source_path=source_path,
        local_path=local_path,
        limits=[MaxFilesCount(2)],
    )

    files = downloader.view_files()
    assert len(files) == limit

    with caplog.at_level(logging.INFO):
        download_result = downloader.run()
        assert "    limits = [" in caplog.text
        assert "        MaxFilesCount(2)," in caplog.text
        assert "    ]" in caplog.text

    assert len(download_result.successful) == limit


def test_downloader_file_limit_is_ignored_by_user_input(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        source_path=source_path,
        local_path=local_path,
        limits=[MaxFilesCount(2)],
    )

    download_result = downloader.run(upload_test_files)

    # limit is not being applied to explicit files list
    assert len(download_result.successful) == len(upload_test_files)


def test_downloader_limit_applied_after_filter(file_all_connections, source_path, upload_test_files, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        source_path=source_path,
        local_path=local_path,
        filters=[Glob("*.csv")],
        limits=[MaxFilesCount(1)],
    )

    excluded = [
        source_path / "exclude_dir/file_4.txt",
        source_path / "exclude_dir/file_5.txt",
        source_path / "news_parse_zp/exclude_dir/file_1.txt",
        source_path / "news_parse_zp/exclude_dir/file_2.txt",
        source_path / "news_parse_zp/exclude_dir/file_3.txt",
    ]

    download_result = downloader.run()

    assert not download_result.failed
    assert not download_result.skipped
    assert not download_result.missing
    assert download_result.successful

    filtered = {
        local_path / file.relative_to(source_path) for file in upload_test_files if os.fspath(file) not in excluded
    }

    # limit should be applied to files which satisfy the filter, not to all files in the source_path
    assert download_result.successful.issubset(filtered)
    assert len(download_result.successful) == 1


def test_downloader_detect_hwm_type_snap_batch_strategy(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        local_path=local_path,
        source_path=local_path,
        hwm_type="file_list",
    )

    with pytest.raises(ValueError, match="`hwm_type` cannot be used in batch strategy"):
        with SnapshotBatchStrategy(step=100500):
            downloader.run()


def test_downloader_detect_hwm_type_inc_batch_strategy(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        local_path=local_path,
        source_path=source_path,
        hwm_type="file_list",
    )

    with pytest.raises(ValueError, match="`hwm_type` cannot be used in batch strategy"):
        with IncrementalBatchStrategy(
            step=timedelta(days=5),
        ):
            downloader.run()


def test_downloader_detect_hwm_type_snapshot_strategy(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
    caplog,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        local_path=local_path,
        source_path=source_path,
        hwm_type="file_list",
    )

    with pytest.raises(ValueError, match="`hwm_type` cannot be used in snapshot strategy"):
        downloader.run()


def test_downloader_file_hwm_strategy_with_wrong_parameters(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
    caplog,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        local_path=local_path,
        source_path=source_path,
        hwm_type="file_list",
    )

    with pytest.raises(ValueError, match="If `hwm_type` is passed you can't specify an `offset`"):
        with IncrementalStrategy(offset=1):
            downloader.run()

    with IncrementalStrategy():
        downloader.run()


@pytest.mark.parametrize(
    "hwm_type",
    [
        "file_list",
        FileListHWM,
    ],
)
def test_downloader_file_hwm_strategy(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
    hwm_type,
):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        local_path=local_path,
        hwm_type=hwm_type,
        source_path=source_path,
    )

    with IncrementalStrategy():
        downloader.run()


@pytest.mark.parametrize(
    "temp_path",
    [
        None,
        os.fspath(Path(tempfile.gettempdir()) / secrets.token_hex(5)),
        Path(tempfile.gettempdir()) / secrets.token_hex(5),
    ],
    ids=["no temp", "temp_path str", "temp_path PurePosixPath"],
)
def test_downloader_with_temp_path(file_all_connections, source_path, upload_test_files, temp_path, tmp_path_factory):
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        source_path=source_path,
        local_path=local_path,
        temp_path=temp_path,
    )

    download_result = downloader.run()

    assert not download_result.failed
    assert not download_result.skipped
    assert not download_result.missing
    assert download_result.successful

    assert sorted(download_result.successful) == sorted(
        local_path / file.relative_to(source_path) for file in upload_test_files
    )

    if temp_path:
        # temp_path is not removed after download is finished,
        # because this may conflict with processes running in parallel
        assert Path(temp_path).is_dir()
