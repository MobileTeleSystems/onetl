import logging
import os
import re
import secrets
import tempfile
from pathlib import Path, PurePosixPath

import pytest

from onetl.exception import DirectoryNotFoundError, NotAFileError
from onetl.file import FileUploader
from onetl.impl import FailedLocalFile, FileExistBehavior, LocalPath, RemoteFile


def test_file_uploader_view_files(file_connection, file_connection_resource_path):
    target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"
    resource_path = file_connection_resource_path

    # upload files
    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
        local_path=resource_path,
    )

    local_files = uploader.view_files()

    local_files_list = []

    for root, _dirs, files in os.walk(resource_path):
        for file in files:
            local_files_list.append(LocalPath(root) / file)

    assert local_files
    assert sorted(local_files) == sorted(local_files_list)


@pytest.mark.parametrize("path_type", [str, PurePosixPath], ids=["path_type str", "path_type PurePosixPath"])
@pytest.mark.parametrize(
    "run_path_type",
    [str, Path],
    ids=["run_path_type str", "run_path_type Path"],
)
@pytest.mark.parametrize("workers", [1, 3, 20])
def test_file_uploader_run_with_files(
    request,
    file_connection,
    file_connection_test_files,
    run_path_type,
    path_type,
    workers,
    caplog,
):
    target_path = path_type(f"/tmp/test_upload_{secrets.token_hex(5)}")
    test_files = file_connection_test_files

    def finalizer():
        file_connection.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    # upload files
    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
        options=FileUploader.Options(
            workers=workers,
        ),
    )

    with caplog.at_level(logging.DEBUG):
        upload_result = uploader.run(run_path_type(file) for file in test_files)

    files_count = len(test_files)
    if 1 <= files_count < workers:
        assert f"Asked for {workers} workers, but there are only {files_count} files" in caplog.text

    if workers > 1 and files_count > 1:
        real_workers = min(workers, files_count)
        assert f"Using ThreadPoolExecutor with {real_workers} workers" in caplog.text
    else:
        assert "Using plain old for-loop" in caplog.text

    assert not upload_result.failed
    assert not upload_result.missing
    assert not upload_result.skipped
    assert upload_result.successful

    assert sorted(upload_result.successful) == sorted(PurePosixPath(target_path) / file.name for file in test_files)

    for remote_file in upload_result.successful:
        assert isinstance(remote_file, RemoteFile)

        assert remote_file.exists()
        assert remote_file.is_file()
        assert not remote_file.is_dir()

        # directory structure is being flattened during upload, restoring it
        local_file = next(file for file in test_files if file.name == remote_file.name)

        # file size is same as expected
        assert file_connection.get_stat(remote_file).st_size == local_file.stat().st_size
        assert file_connection.get_stat(remote_file).st_size == remote_file.stat().st_size

        # file content is same as expected
        assert file_connection.read_bytes(remote_file) == local_file.read_bytes()


@pytest.mark.parametrize("path_type", [str, PurePosixPath], ids=["path_type str", "path_type Path"])
def test_file_uploader_run_with_local_path(request, file_connection, file_connection_resource_path, path_type):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    resource_path = file_connection_resource_path

    def finalizer():
        file_connection.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    # upload files
    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
        local_path=path_type(resource_path),
    )

    upload_result = uploader.run()

    assert not upload_result.failed
    assert not upload_result.missing
    assert not upload_result.skipped
    assert upload_result.successful

    local_files_list = []

    for root, _, files in os.walk(resource_path):
        for file_name in files:
            local_files_list.append(Path(root) / file_name)

    assert sorted(path for path in upload_result.successful) == sorted(
        Path(target_path) / file.relative_to(resource_path) for file in local_files_list
    )

    for remote_file in upload_result.successful:
        assert remote_file.exists()
        assert remote_file.is_file()
        assert not remote_file.is_dir()

        # directory structure is being flattened during upload, restoring it
        local_file = next(file for file in local_files_list if file.name == remote_file.name)

        # file size is same as expected
        assert file_connection.get_stat(remote_file).st_size == local_file.stat().st_size
        assert file_connection.get_stat(remote_file).st_size == remote_file.stat().st_size

        # file content is same as expected
        assert file_connection.read_bytes(remote_file) == local_file.read_bytes()


def test_file_uploader_run_missing_file(request, file_connection, file_connection_test_files, caplog):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    test_files = file_connection_test_files

    def finalizer():
        file_connection.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    # upload files
    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
    )

    missing_file = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    with caplog.at_level(logging.WARNING):
        upload_result = uploader.run(test_files + [missing_file])

        assert f"Missing file '{missing_file}', skipping" in caplog.text

    assert not upload_result.failed
    assert not upload_result.skipped

    assert upload_result.successful
    assert len(upload_result.successful) == len(test_files)

    assert upload_result.missing
    assert len(upload_result.missing) == 1
    assert upload_result.missing == {missing_file}

    for missing_file in upload_result.missing:
        assert isinstance(missing_file, LocalPath)
        assert not missing_file.exists()


def test_file_uploader_run_delete_local(
    request,
    file_connection,
    file_connection_resource_path,
    file_connection_test_files,
    caplog,
):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    resource_path = file_connection_resource_path
    test_files = file_connection_test_files

    def finalizer():
        file_connection.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
        options=FileUploader.Options(delete_local=True),
    )

    local_files_list = []
    local_files_stat = {}
    local_files_bytes = {}

    for root, _, files in os.walk(resource_path):
        for file_name in files:
            local_file = Path(root) / file_name
            local_files_list.append(local_file)
            local_files_stat[local_file] = local_file.stat()
            local_files_bytes[local_file] = local_file.read_bytes()

    with caplog.at_level(logging.WARNING):
        upload_result = uploader.run(test_files)

        assert "LOCAL FILES WILL BE PERMANENTLY DELETED AFTER UPLOADING !!!" in caplog.text

    assert not upload_result.failed
    assert not upload_result.skipped
    assert not upload_result.missing
    assert upload_result.successful

    assert sorted(upload_result.successful) == sorted(target_path / file.name for file in test_files)

    existing_files = []
    for root, _dirs, files in os.walk(resource_path):
        for file_name in files:
            existing_files.append(Path(root) / file_name)

    for remote_file in upload_result.successful:
        assert isinstance(remote_file, RemoteFile)

        assert remote_file.exists()
        assert remote_file.is_file()
        assert not remote_file.is_dir()

        # directory structure is being flattened during upload, restoring it
        local_file = next(file for file in test_files if file.name == remote_file.name)

        # file size is same as expected
        assert file_connection.get_stat(remote_file).st_size == local_files_stat[local_file].st_size
        assert file_connection.get_stat(remote_file).st_size == remote_file.stat().st_size

        # file content is same as expected
        assert file_connection.read_bytes(remote_file) == local_files_bytes[local_file]

        # uploaded file is removed
        assert local_file not in existing_files
        assert not local_file.exists()

    # skipped files are left intact
    assert existing_files


def test_file_uploader_run_mode_fail(request, file_connection, file_connection_test_files):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    test_files = file_connection_test_files

    # make copy of files to upload in the target_path
    remote_files = []
    for test_file in test_files:
        remote_file = target_path / test_file.name
        remote_files.append(file_connection.write_text(remote_file, "unchanged"))

    def finalizer():
        file_connection.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    # upload changed files
    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
        options=FileUploader.Options(if_exists=FileExistBehavior.ERROR),
    )

    upload_result = uploader.run(test_files)

    assert not upload_result.successful
    assert not upload_result.missing
    assert not upload_result.skipped
    assert upload_result.failed

    assert sorted(upload_result.failed) == sorted(test_files)

    for local_file in upload_result.failed:
        assert isinstance(local_file, FailedLocalFile)

        assert local_file.exists()
        assert local_file.is_file()
        assert not local_file.is_dir()

        assert isinstance(local_file.exception, FileExistsError)

        remote_file = remote_files[remote_files.index(target_path / local_file.name)]
        assert re.search(rf"File '{remote_file}' \(kind='file', .*\) already exists", str(local_file.exception))

        # file size wasn't changed
        assert file_connection.get_stat(remote_file).st_size != local_file.stat().st_size
        assert file_connection.get_stat(remote_file).st_size == remote_file.stat().st_size

        # file content wasn't changed
        assert file_connection.read_text(remote_file) == "unchanged"


def test_file_uploader_run_mode_skip_file(request, file_connection, file_connection_test_files, caplog):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    test_files = file_connection_test_files

    # make copy of files to upload in the target_path
    remote_files = []
    for test_file in test_files:
        remote_file = target_path / test_file.name
        remote_files.append(file_connection.write_text(remote_file, "unchanged"))

    def finalizer():
        file_connection.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    # upload changed files
    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
        options=FileUploader.Options(if_exists=FileExistBehavior.IGNORE),
    )

    with caplog.at_level(logging.WARNING):
        upload_result = uploader.run(test_files)

        for file in remote_files:
            assert re.search(rf"File '{file}' \(kind='file', .*\) already exists, skipping", caplog.text)

    assert not upload_result.successful
    assert not upload_result.missing
    assert not upload_result.failed
    assert upload_result.skipped

    assert sorted(upload_result.skipped) == sorted(test_files)

    for local_file in upload_result.skipped:
        assert isinstance(local_file, LocalPath)

        assert local_file.exists()
        assert local_file.is_file()
        assert not local_file.is_dir()

        remote_file = remote_files[remote_files.index(target_path / local_file.name)]

        # file size wasn't changed
        assert file_connection.get_stat(remote_file).st_size != local_file.stat().st_size
        assert file_connection.get_stat(remote_file).st_size == remote_file.stat().st_size

        # file content wasn't changed
        assert file_connection.read_text(remote_file) == "unchanged"


def test_file_uploader_run_mode_replace_file(request, file_connection, file_connection_test_files, caplog):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    test_files = file_connection_test_files

    # make copy of files to upload in the target_path
    remote_files = []
    for test_file in test_files:
        remote_file = target_path / test_file.name
        remote_files.append(file_connection.write_text(remote_file, "unchanged"))

    def finalizer():
        file_connection.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    # upload changed files
    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
        options=FileUploader.Options(if_exists=FileExistBehavior.REPLACE_FILE),
    )

    with caplog.at_level(logging.WARNING):
        upload_result = uploader.run(test_files)

        for target_file in remote_files:
            assert re.search(rf"File '{target_file}' \(kind='file', .*\) already exists, overwriting", caplog.text)

    assert not upload_result.failed
    assert not upload_result.skipped
    assert not upload_result.missing
    assert upload_result.successful

    assert sorted(upload_result.successful) == sorted(PurePosixPath(file) for file in remote_files)

    for remote_file in upload_result.successful:
        assert remote_file.exists()
        assert remote_file.is_file()
        assert not remote_file.is_dir()

        old_remote_file = remote_files[remote_files.index(target_path / remote_file.name)]

        # directory structure is being flattened during upload, restoring it
        local_file = next(file for file in test_files if file.name == remote_file.name)

        # file size was changed
        assert file_connection.get_stat(remote_file).st_size != old_remote_file.stat().st_size
        assert file_connection.get_stat(remote_file).st_size == local_file.stat().st_size
        assert file_connection.get_stat(remote_file).st_size == remote_file.stat().st_size

        # file content was changed
        assert file_connection.read_text(remote_file) != "unchanged"
        assert file_connection.read_bytes(remote_file) == local_file.read_bytes()


@pytest.mark.parametrize("remote_dir_exist", [True, False])
def test_file_uploader_run_mode_replace_entire_directory(
    request,
    file_connection,
    file_connection_test_files,
    remote_dir_exist,
    caplog,
):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    test_files = file_connection_test_files

    # make copy of files to upload in the target_path
    temp_file = target_path / secrets.token_hex(5)

    if remote_dir_exist:
        file_connection.write_text(temp_file, "abc")

    def finalizer():
        file_connection.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
        options=FileUploader.Options(if_exists=FileExistBehavior.REPLACE_ENTIRE_DIRECTORY),
    )

    with caplog.at_level(logging.WARNING):
        upload_result = uploader.run(test_files)
        assert "TARGET DIRECTORY WILL BE CLEANED UP BEFORE UPLOADING FILES !!!" in caplog.text

    assert not upload_result.failed
    assert not upload_result.skipped
    assert not upload_result.missing
    assert upload_result.successful

    target_path_content = []
    for root, _dirs, files in file_connection.walk(target_path):
        target_path_content.extend(root / file for file in files)

    # target path contains only downloaded files
    assert sorted(target_path_content) == sorted(upload_result.successful)
    assert not file_connection.path_exists(temp_file)


def test_file_uploader_run_local_path_does_not_exist(file_connection, tmp_path_factory):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    local_path_parent = tmp_path_factory.mktemp("local_path")
    local_path = local_path_parent / "abc"

    uploader = FileUploader(connection=file_connection, target_path=target_path, local_path=local_path)

    with pytest.raises(DirectoryNotFoundError, match=f"'{local_path}' does not exist"):
        uploader.run()


def test_file_uploader_run_local_path_not_a_directory(file_connection):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    with tempfile.NamedTemporaryFile() as file:
        uploader = FileUploader(connection=file_connection, target_path=target_path, local_path=file.name)

        with pytest.raises(NotADirectoryError, match=rf"'{file.name}' \(kind='file', .*\) is not a directory"):
            uploader.run()


def test_file_uploader_run_target_path_not_a_directory(request, file_connection, file_connection_resource_path):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    resource_path = file_connection_resource_path
    file_connection.write_text(target_path, "abc")

    def finalizer():
        file_connection.remove_file(target_path)

    request.addfinalizer(finalizer)

    uploader = FileUploader(connection=file_connection, target_path=target_path, local_path=resource_path)

    with pytest.raises(NotADirectoryError, match=rf"'{target_path}' \(kind='file', .*\) is not a directory"):
        uploader.run()


def test_file_uploader_run_input_is_not_file(file_connection):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    # upload files
    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
    )

    with tempfile.TemporaryDirectory() as not_a_file:
        with pytest.raises(NotAFileError, match=rf"'{not_a_file}' \(kind='directory', .*\) is not a file"):
            uploader.run([not_a_file])


@pytest.mark.parametrize(
    "pass_local_path",
    [False, True],
    ids=["Without local_path", "With local_path"],
)
def test_file_uploader_run_with_empty_files(file_connection, pass_local_path, tmp_path_factory, caplog):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    local_path = tmp_path_factory.mktemp("local_path")

    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
        local_path=local_path if pass_local_path else None,
    )

    with caplog.at_level(logging.INFO):
        upload_result = uploader.run([])  # argument takes precedence over source_path content

    assert "No files to upload!" in caplog.text
    assert "Starting the upload process" not in caplog.text

    assert not upload_result.failed
    assert not upload_result.skipped
    assert not upload_result.missing
    assert not upload_result.successful


def test_file_uploader_run_with_empty_local_path(request, file_connection, tmp_path_factory, caplog):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    local_path = tmp_path_factory.mktemp("local_path")

    def finalizer():
        file_connection.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
        local_path=local_path,
    )

    with caplog.at_level(logging.INFO):
        upload_result = uploader.run()

    assert "No files to upload!" in caplog.text
    assert "Starting the upload process" not in caplog.text

    assert not upload_result.failed
    assert not upload_result.skipped
    assert not upload_result.missing
    assert not upload_result.successful


def test_file_uploader_without_files_and_without_local_path(file_connection):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    uploader = FileUploader(connection=file_connection, target_path=target_path)

    with pytest.raises(ValueError, match="Neither file list nor `local_path` are passed"):
        uploader.run()


def test_file_uploader_run_with_relative_files_and_local_path(
    request,
    file_connection,
    file_connection_resource_path,
    caplog,
):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    resource_path = file_connection_resource_path

    def finalizer():
        file_connection.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    # upload files
    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
        local_path=resource_path,
    )

    local_files_list = []
    for root, _, files in os.walk(resource_path):
        for file_name in files:
            abs_path_file = Path(root) / file_name
            local_files_list.append(abs_path_file.relative_to(resource_path))

    with caplog.at_level(logging.WARNING):
        upload_result = uploader.run(local_files_list)
        assert ("Passed both `local_path` and files list at the same time. Using explicit files list") in caplog.text

    assert not upload_result.failed
    assert not upload_result.missing
    assert upload_result.successful
    assert sorted(path for path in upload_result.successful) == sorted(
        PurePosixPath(target_path) / file for file in local_files_list
    )

    for remote_file in upload_result.successful:
        assert remote_file.exists()
        assert remote_file.is_file()
        assert not remote_file.is_dir()

        local_file = resource_path / remote_file.relative_to(target_path)

        # file size is same as expected
        assert file_connection.get_stat(remote_file).st_size == local_file.stat().st_size
        assert file_connection.get_stat(remote_file).st_size == remote_file.stat().st_size

        # file content is same as expected
        assert file_connection.read_bytes(remote_file) == local_file.read_bytes()


def test_file_uploader_run_with_absolute_files_and_local_path(
    request,
    file_connection,
    file_connection_resource_path,
    caplog,
):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    resource_path = file_connection_resource_path

    def finalizer():
        file_connection.remove_dir(target_path, recursive=True)

    request.addfinalizer(finalizer)

    # upload files
    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
        local_path=resource_path,
    )
    local_files_list = []

    for root, _, files in os.walk(resource_path):
        for file_name in files:
            local_files_list.append(Path(root) / file_name)

    with caplog.at_level(logging.WARNING):
        upload_result = uploader.run(local_files_list)
        assert ("Passed both `local_path` and files list at the same time. Using explicit files list") in caplog.text

    assert not upload_result.failed
    assert not upload_result.missing
    assert upload_result.successful
    assert sorted(path for path in upload_result.successful) == sorted(
        PurePosixPath(target_path) / file.relative_to(resource_path) for file in local_files_list
    )

    for remote_file in upload_result.successful:
        assert remote_file.exists()
        assert remote_file.is_file()
        assert not remote_file.is_dir()

        local_file = resource_path / remote_file.relative_to(target_path)

        # file size is same as expected
        assert file_connection.get_stat(remote_file).st_size == local_file.stat().st_size
        assert file_connection.get_stat(remote_file).st_size == remote_file.stat().st_size

        # file content is same as expected
        assert file_connection.read_bytes(remote_file) == local_file.read_bytes()


def test_file_uploader_run_absolute_path_not_match_local_path(file_connection, file_connection_resource_path):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    resource_path = file_connection_resource_path

    uploader = FileUploader(connection=file_connection, target_path=target_path, local_path=resource_path)

    with pytest.raises(ValueError, match=f"File path '/some/path/1' does not match source_path '{resource_path}'"):
        uploader.run(["/some/path/1", "/some/path/2"])


def test_file_uploader_run_relative_paths_without_local_path(file_connection):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

    uploader = FileUploader(connection=file_connection, target_path=target_path)

    with pytest.raises(ValueError, match="Cannot pass relative file path with empty `local_path`"):
        uploader.run(["some/path/1", "some/path/2"])


@pytest.mark.parametrize(
    "temp_path",
    [
        None,
        "/tmp/" + secrets.token_hex(5),
        PurePosixPath("/tmp") / secrets.token_hex(5),
    ],
    ids=["no temp", "temp_path str", "temp_path PurePosixPath"],
)
def test_file_uploader_run_with_temp_path(file_connection, file_connection_test_files, temp_path):
    target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
    test_files = file_connection_test_files

    # upload files
    uploader = FileUploader(
        connection=file_connection,
        target_path=target_path,
        temp_path=temp_path,
    )

    upload_result = uploader.run(test_files)

    assert not upload_result.failed
    assert not upload_result.missing
    assert not upload_result.skipped
    assert upload_result.successful

    assert sorted(upload_result.successful) == sorted(target_path / file.name for file in test_files)

    if temp_path and file_connection.path_exists(temp_path):
        # temp_path is not removed after upload is finished,
        # because this may conflict with processes running in parallel
        file_connection.is_dir(temp_path)
