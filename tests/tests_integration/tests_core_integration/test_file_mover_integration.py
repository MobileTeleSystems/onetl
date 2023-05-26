import logging
import os
import re
import secrets
from pathlib import Path, PurePosixPath

import pytest
from pytest_lazyfixture import lazy_fixture

from onetl.core import FileMover
from onetl.exception import DirectoryNotFoundError, NotAFileError
from onetl.file.filter import ExcludeDir, Glob
from onetl.file.limit import MaxFilesCount
from onetl.impl import FailedRemoteFile, FileWriteMode, RemoteFile, RemotePath


def test_mover_view_file(file_all_connections, source_path, upload_test_files):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
    )

    remote_files = mover.view_files()
    remote_files_list = []

    for root, _dirs, files in file_all_connections.walk(source_path):
        for file in files:
            remote_files_list.append(RemotePath(root) / file)

    assert remote_files
    assert sorted(remote_files) == sorted(remote_files_list)


@pytest.mark.parametrize("path_type", [str, PurePosixPath], ids=["path_type str", "path_type PurePosixPath"])
def test_mover_run(
    request,
    file_all_connections,
    source_path,
    upload_test_files,
    path_type,
):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    def finalizer():
        file_all_connections.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    mover = FileMover(
        connection=file_all_connections,
        source_path=path_type(source_path),
        target_path=path_type(target_path),
    )

    # record files content and size before move
    files_content = {}
    files_size = {}
    for root, _dirs, files in file_all_connections.walk(source_path):
        for file_name in files:
            file_path = root / file_name
            files_content[file_path] = file_all_connections.read_bytes(file_path)
            files_size[file_path] = file_all_connections.get_stat(file_path).st_size

    move_result = mover.run()

    assert not move_result.failed
    assert not move_result.skipped
    assert not move_result.missing
    assert move_result.successful

    assert sorted(move_result.successful) == sorted(
        target_path / file.relative_to(source_path) for file in upload_test_files
    )

    for target_file in move_result.successful:
        assert isinstance(target_file, RemoteFile)
        old_path = source_path / target_file.relative_to(target_path)

        assert file_all_connections.resolve_file(target_file)

        # file size is same as expected
        assert file_all_connections.get_stat(target_file).st_size == files_size[old_path]

        # file content is same as expected
        assert file_all_connections.read_bytes(target_file) == files_content[old_path]


@pytest.mark.parametrize("path_type", [str, Path])
def test_mover_file_filter_exclude_dir(
    request,
    file_all_connections,
    source_path,
    upload_test_files,
    path_type,
    caplog,
):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    def finalizer():
        file_all_connections.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
        filters=[ExcludeDir(path_type(source_path / "exclude_dir"))],
    )

    excluded = [
        source_path / "exclude_dir/file_4.txt",
        source_path / "exclude_dir/file_5.txt",
    ]

    with caplog.at_level(logging.INFO):
        move_result = mover.run()
        assert "    filters = [" in caplog.text
        assert f"        ExcludeDir('{source_path}/exclude_dir')," in caplog.text
        assert "    ]" in caplog.text

    assert not move_result.failed
    assert not move_result.skipped
    assert not move_result.missing
    assert move_result.successful

    assert sorted(move_result.successful) == sorted(
        target_path / file.relative_to(source_path) for file in upload_test_files if file not in excluded
    )


def test_mover_file_filter_glob(request, file_all_connections, source_path, upload_test_files, caplog):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    def finalizer():
        file_all_connections.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
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
        move_result = mover.run()
        assert "    filters = [" in caplog.text
        assert "        Glob('*.csv')," in caplog.text
        assert "    ]" in caplog.text

    assert not move_result.failed
    assert not move_result.skipped
    assert not move_result.missing
    assert move_result.successful

    assert sorted(move_result.successful) == sorted(
        target_path / file.relative_to(source_path) for file in upload_test_files if file not in excluded
    )


def test_mover_file_filter_is_ignored_by_user_input(
    request,
    file_all_connections,
    source_path,
    upload_test_files,
):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    def finalizer():
        file_all_connections.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
        filters=[Glob("*.csv")],
    )

    move_result = mover.run(upload_test_files)

    # filter is not being applied to explicit files list
    assert sorted(move_result.successful) == sorted(
        target_path / file.relative_to(source_path) for file in upload_test_files
    )


@pytest.mark.parametrize(
    "source_path_value",
    [None, lazy_fixture("source_path")],
    ids=["Without source_path", "With source path"],
)
def test_mover_run_with_files_absolute(
    request,
    file_all_connections,
    source_path,
    upload_test_files,
    source_path_value,
    caplog,
):
    target_path = RemotePath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    def finalizer():
        file_all_connections.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path_value,
        target_path=target_path,
    )

    # record files content and size before move
    files_content = {}
    files_size = {}
    for root, _dirs, files in file_all_connections.walk(source_path):
        for file_name in files:
            file_path = root / file_name
            files_content[file_path] = file_all_connections.read_bytes(file_path)
            files_size[file_path] = file_all_connections.get_stat(file_path).st_size

    with caplog.at_level(logging.WARNING):
        move_result = mover.run(upload_test_files)

        if source_path_value:
            assert (
                "Passed both `source_path` and files list at the same time. Using explicit files list"
            ) in caplog.text

    assert not move_result.failed
    assert not move_result.skipped
    assert not move_result.missing
    assert move_result.successful

    if source_path_value:
        target_files = [target_path / file.relative_to(source_path) for file in upload_test_files]
    else:
        # no source path - do not preserve folder structure
        target_files = [target_path / file.name for file in upload_test_files]

    assert sorted(move_result.successful) == sorted(target_files)

    for old_file in upload_test_files:
        if source_path_value:
            target_file = target_path / old_file.relative_to(source_path)
        else:
            target_file = target_path / old_file.name

        assert file_all_connections.resolve_file(target_file)

        # file size is same as expected
        assert file_all_connections.get_stat(target_file).st_size == files_size[old_file]

        # file content is same as expected
        assert file_all_connections.read_bytes(target_file) == files_content[old_file]


def test_mover_run_with_files_relative_and_source_path(
    request,
    file_all_connections,
    source_path,
    upload_test_files,
):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    def finalizer():
        file_all_connections.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    relative_files_path = [file.relative_to(source_path) for file in upload_test_files]

    # record files content and size before move
    files_content = {}
    files_size = {}
    for root, _dirs, files in file_all_connections.walk(source_path):
        for file_name in files:
            file_path = root / file_name
            files_content[file_path] = file_all_connections.read_bytes(file_path)
            files_size[file_path] = file_all_connections.get_stat(file_path).st_size

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
    )

    move_result = mover.run(file for file in relative_files_path)

    assert not move_result.failed
    assert not move_result.skipped
    assert not move_result.missing
    assert move_result.successful

    assert sorted(move_result.successful) == sorted(target_path / file for file in relative_files_path)

    for old_file in upload_test_files:
        target_file = target_path / old_file.relative_to(source_path)

        assert file_all_connections.resolve_file(target_file)

        # file size is same as expected
        assert file_all_connections.get_stat(target_file).st_size == files_size[old_file]

        # file content is same as expected
        assert file_all_connections.read_bytes(target_file) == files_content[old_file]


def test_mover_run_without_files_and_source_path(file_all_connections):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    mover = FileMover(
        connection=file_all_connections,
        target_path=target_path,
    )
    with pytest.raises(ValueError, match="Neither file list nor `source_path` are passed"):
        mover.run()


@pytest.mark.parametrize(
    "pass_source_path",
    [False, True],
    ids=["Without source_path", "With source_path"],
)
def test_mover_run_with_empty_files_input(
    request,
    file_all_connections,
    pass_source_path,
    upload_test_files,
    source_path,
):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    def finalizer():
        file_all_connections.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    mover = FileMover(
        connection=file_all_connections,
        target_path=target_path,
        source_path=source_path if pass_source_path else None,
    )

    move_result = mover.run([])  # this argument takes precedence

    assert not move_result.failed
    assert not move_result.skipped
    assert not move_result.missing
    assert not move_result.successful


def test_mover_run_with_empty_source_path(request, file_all_connections):
    source_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    file_all_connections.create_dir(source_path)
    if not file_all_connections.path_exists(source_path):
        # S3 does not support creating directories
        return

    def finalizer1():
        file_all_connections.remove_dir(source_path, recursive=True)

    request.addfinalizer(finalizer1)

    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    def finalizer2():
        file_all_connections.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer2)

    mover = FileMover(
        connection=file_all_connections,
        target_path=target_path,
        source_path=source_path,
    )

    move_result = mover.run()

    assert not move_result.failed
    assert not move_result.skipped
    assert not move_result.missing
    assert not move_result.successful


def test_mover_run_relative_path_without_source_path(file_all_connections):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    mover = FileMover(
        connection=file_all_connections,
        target_path=target_path,
    )

    with pytest.raises(ValueError, match="Cannot pass relative file path with empty `source_path`"):
        mover.run(["some/relative/path/file.txt"])


def test_mover_run_absolute_path_not_match_source_path(
    file_all_connections,
    source_path,
    upload_test_files,
):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
    )

    error_message = f"File path '/some/relative/path/file.txt' does not match source_path '{source_path}'"
    with pytest.raises(ValueError, match=error_message):
        mover.run(["/some/relative/path/file.txt"])


@pytest.mark.parametrize(
    "options",
    [{"mode": "error"}, FileMover.Options(mode="error"), FileMover.Options(mode=FileWriteMode.ERROR)],
)
def test_mover_mode_error(request, file_all_connections, source_path, upload_test_files, options):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    def finalizer():
        file_all_connections.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    # create target files before move
    target_files_size = {}
    for test_file in upload_test_files:
        target_file = target_path / test_file.relative_to(source_path)

        file_all_connections.write_text(target_file, "unchanged")
        target_files_size[target_file] = file_all_connections.get_stat(target_file).st_size

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
        options=options,
    )

    move_result = mover.run()

    assert not move_result.successful
    assert not move_result.missing
    assert not move_result.skipped
    assert move_result.failed

    assert sorted(move_result.failed) == sorted(upload_test_files)

    for source_file in move_result.failed:
        assert isinstance(source_file, FailedRemoteFile)
        assert file_all_connections.resolve_file(source_file)

        assert isinstance(source_file.exception, FileExistsError)
        target_file = target_path / source_file.relative_to(source_path)
        assert re.search(rf"File '{target_file}' \(kind='file', .*\) already exists", str(source_file.exception))

        # file size wasn't changed
        assert file_all_connections.get_stat(target_file).st_size != source_file.stat().st_size
        assert file_all_connections.get_stat(target_file).st_size == target_files_size[target_file]

        # file content wasn't changed
        assert file_all_connections.read_text(target_file) == "unchanged"


def test_mover_mode_ignore(request, file_all_connections, source_path, upload_test_files, caplog):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    def finalizer():
        file_all_connections.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    # create target files before move
    target_files = []
    target_files_size = {}
    for test_file in upload_test_files:
        target_file = target_path / test_file.relative_to(source_path)

        file_all_connections.write_text(target_file, "unchanged")
        target_files.append(target_file)
        target_files_size[target_file] = file_all_connections.get_stat(target_file).st_size

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
        options=FileMover.Options(mode=FileWriteMode.IGNORE),
    )

    with caplog.at_level(logging.WARNING):
        move_result = mover.run()

        for file in target_files:
            assert re.search(rf"File '{file}' \(kind='file', .*\) already exists, skipping", caplog.text)

    assert not move_result.successful
    assert not move_result.failed
    assert not move_result.missing
    assert move_result.skipped

    assert sorted(move_result.skipped) == sorted(upload_test_files)

    for source_file in move_result.skipped:
        assert isinstance(source_file, RemoteFile)
        assert file_all_connections.resolve_file(source_file)

        target_file = target_path / source_file.relative_to(source_path)

        # file size wasn't changed
        assert file_all_connections.get_stat(target_file).st_size != source_file.stat().st_size
        assert file_all_connections.get_stat(target_file).st_size == target_files_size[target_file]

        # file content wasn't changed
        assert file_all_connections.read_text(target_file) == "unchanged"


def test_mover_mode_overwrite(request, file_all_connections, source_path, upload_test_files, caplog):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    def finalizer():
        file_all_connections.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    # create target files before move
    target_files = []
    target_files_size = {}
    source_files_size = {}
    source_files_content = {}
    for test_file in upload_test_files:
        source_files_size[test_file] = file_all_connections.get_stat(test_file).st_size
        source_files_content[test_file] = file_all_connections.read_text(test_file)
        target_file = target_path / test_file.relative_to(source_path)

        file_all_connections.write_text(target_file, "unchanged")
        target_files.append(target_file)
        target_files_size[target_file] = file_all_connections.get_stat(target_file).st_size

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
        options=FileMover.Options(mode=FileWriteMode.OVERWRITE),
    )

    with caplog.at_level(logging.WARNING):
        move_result = mover.run()

        for changed_file in target_files:
            assert re.search(rf"File '{changed_file}' \(kind='file', .*\) already exists, overwriting", caplog.text)

    assert not move_result.failed
    assert not move_result.missing
    assert not move_result.skipped
    assert move_result.successful

    assert sorted(move_result.successful) == sorted(
        target_path / file.relative_to(source_path) for file in upload_test_files
    )

    for source_file in upload_test_files:
        target_file = target_path / source_file.relative_to(source_path)

        assert file_all_connections.resolve_file(target_file)
        assert not file_all_connections.path_exists(source_file)

        # file size was changed
        assert file_all_connections.get_stat(target_file).st_size != target_files_size[target_file]
        assert file_all_connections.get_stat(target_file).st_size == source_files_size[source_file]

        # file content was changed
        assert file_all_connections.read_text(target_file) != "unchanged"
        assert file_all_connections.read_text(target_file) == source_files_content[source_file]


@pytest.mark.parametrize("remote_dir_exist", [True, False])
def test_mover_mode_delete_all(
    request,
    file_all_connections,
    source_path,
    upload_test_files,
    remote_dir_exist,
    caplog,
):
    target_path = RemotePath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    def finalizer():
        file_all_connections.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    temp_file = target_path / secrets.token_hex(5)
    if remote_dir_exist:
        file_all_connections.write_text(temp_file, "unchanged")

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
        options=FileMover.Options(mode=FileWriteMode.DELETE_ALL),
    )

    with caplog.at_level(logging.WARNING):
        move_result = mover.run()
        assert "TARGET DIRECTORY WILL BE CLEANED UP BEFORE MOVING FILES !!!" in caplog.text

    assert not move_result.failed
    assert not move_result.missing
    assert not move_result.skipped
    assert move_result.successful

    # folder contains only moved files
    content = (root / file.name for root, _dirs, files in file_all_connections.walk(target_path) for file in files)
    assert sorted(content) == sorted(move_result.successful)
    assert not file_all_connections.path_exists(temp_file)


def test_mover_run_missing_file(request, file_all_connections, upload_test_files, caplog):
    target_path = RemotePath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    file_all_connections.create_dir(target_path)

    def finalizer():
        file_all_connections.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    # upload files
    mover = FileMover(
        connection=file_all_connections,
        target_path=target_path,
    )

    missing_file = target_path / "missing"

    with caplog.at_level(logging.WARNING):
        move_result = mover.run(upload_test_files + [missing_file])

        assert f"Missing file '{missing_file}', skipping" in caplog.text

    assert not move_result.failed
    assert not move_result.skipped
    assert move_result.missing
    assert move_result.successful

    assert len(move_result.successful) == len(upload_test_files)
    assert len(move_result.missing) == 1

    assert move_result.missing == {missing_file}
    assert isinstance(move_result.missing[0], RemotePath)


def test_mover_source_path_does_not_exist(file_all_connections):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"
    source_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
    )

    with pytest.raises(DirectoryNotFoundError, match=f"'{source_path}' does not exist"):
        mover.run()


def test_mover_source_path_not_a_directory(request, file_all_connections):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    def finalizer1():
        file_all_connections.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer1)

    source_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    file_all_connections.write_text(source_path, "abc")

    def finalizer2():
        file_all_connections.remove_file(source_path)

    request.addfinalizer(finalizer2)

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
    )

    with pytest.raises(NotADirectoryError, match=rf"'{source_path}' \(kind='file', .*\) is not a directory"):
        mover.run()


def test_mover_target_path_not_a_directory(request, file_all_connections):
    source_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    file_all_connections.create_dir(source_path)

    def finalizer1():
        file_all_connections.remove_dir(source_path, recursive=True)

    request.addfinalizer(finalizer1)

    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    file_all_connections.write_text(target_path, "")

    def finalizer2():
        file_all_connections.remove_file(target_path)

    request.addfinalizer(finalizer2)

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
    )

    with pytest.raises(NotADirectoryError, match=rf"'{target_path}' \(kind='file', .*\) is not a directory"):
        mover.run()


def test_mover_run_input_is_not_file(request, file_all_connections):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    source_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    not_a_file = source_path / "not_a_file"

    file_all_connections.create_dir(not_a_file)

    if not file_all_connections.path_exists(not_a_file):
        # S3 does not support creating directories
        return

    def finalizer():
        file_all_connections.remove_dir(source_path, recursive=True)

    request.addfinalizer(finalizer)

    mover = FileMover(
        connection=file_all_connections,
        target_path=target_path,
    )

    with pytest.raises(NotAFileError, match=rf"'{not_a_file}' \(kind='directory', .*\) is not a file"):
        mover.run([not_a_file])


def test_mover_file_limit_custom(file_all_connections, source_path, upload_test_files, caplog):
    limit = 2
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
        limits=[MaxFilesCount(2)],
    )

    files = mover.view_files()
    assert len(files) == limit

    with caplog.at_level(logging.INFO):
        move_result = mover.run()
        assert "    limits = [" in caplog.text
        assert "        MaxFilesCount(2)," in caplog.text
        assert "    ]" in caplog.text

    assert len(move_result.successful) == limit


def test_mover_file_limit_is_ignored_by_user_input(
    file_all_connections,
    source_path,
    upload_test_files,
):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
        limits=[MaxFilesCount(2)],
    )

    move_result = mover.run(upload_test_files)

    # limit is not being applied to explicit files list
    assert len(move_result.successful) == len(upload_test_files)


def test_mover_limit_applied_after_filter(file_all_connections, source_path, upload_test_files):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

    mover = FileMover(
        connection=file_all_connections,
        source_path=source_path,
        target_path=target_path,
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

    move_result = mover.run()

    assert not move_result.failed
    assert not move_result.skipped
    assert not move_result.missing
    assert move_result.successful

    filtered = {
        target_path / file.relative_to(source_path) for file in upload_test_files if os.fspath(file) not in excluded
    }

    # limit should be applied to files which satisfy the filter, not to all files in the source_path
    assert move_result.successful.issubset(filtered)
    assert len(move_result.successful) == 1
