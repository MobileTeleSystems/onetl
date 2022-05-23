import logging
import os
import secrets
import shutil
import tempfile
from datetime import datetime
from pathlib import Path, PurePosixPath

import pytest
from etl_entities import Process

from onetl.connection import FileConnection, FileWriteMode
from onetl.core import FileUploader
from onetl.exception import DirectoryNotFoundError


class TestUploader:
    @pytest.mark.parametrize("path_type", [str, PurePosixPath], ids=["path_type str", "path_type Path"])
    @pytest.mark.parametrize(
        "run_path_type",
        [str, Path],
        ids=["run_path_type str", "run_path_type Path"],
    )
    def test_run(self, file_connection, test_files, run_path_type, path_type):
        target_path = path_type(f"/tmp/test_upload_{secrets.token_hex(5)}")

        # upload files
        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
        )

        upload_result = uploader.run(run_path_type(file) for file in test_files)

        assert not upload_result.failed
        assert not upload_result.missing

        assert upload_result.success
        assert len(upload_result.success) == len(test_files)
        assert upload_result.success == {Path(target_path) / file.name for file in test_files}

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)

            for success_file in upload_result.success:
                test_file = next(file for file in test_files if file.name == success_file.name)
                assert success_file == Path(target_path) / test_file.name

                remote_file = PurePosixPath(target_path) / success_file.name

                # file size is same as expected
                assert file_connection.get_stat(remote_file).st_size == test_file.stat().st_size
                assert file_connection.get_stat(remote_file).st_mtime >= test_file.stat().st_mtime

                # file content is same as expected
                temp_file = temp_root / success_file.name
                file_connection.download_file(remote_file, temp_file)
                assert temp_file.read_bytes() == test_file.read_bytes()

    def test_view_files(self, file_connection, resource_path):
        target_path = f"/tmp/test_upload_{secrets.token_hex(5)}"

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
                local_files_list.append(Path(root) / file)

        assert local_files
        assert sorted(local_files) == sorted(local_files_list)

    @pytest.mark.parametrize("path_type", [str, PurePosixPath], ids=["path_type str", "path_type Path"])
    def test_run_with_local_path(self, file_connection, resource_path, path_type):
        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

        # upload files
        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
            local_path=path_type(resource_path),
        )

        upload_result = uploader.run()

        assert not upload_result.failed
        assert not upload_result.missing

        local_files_list = []

        for root, _, files in os.walk(resource_path):
            for file_name in files:
                local_files_list.append(Path(root) / file_name)

        assert upload_result.success
        assert len(upload_result.success) == len(local_files_list)
        assert sorted(path for path in upload_result.success) == sorted(
            {Path(target_path) / file.relative_to(resource_path) for file in local_files_list},
        )  # in order to be able to compare

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)

            for success_file in upload_result.success:
                test_file = next(file for file in local_files_list if file.name == success_file.name)
                assert success_file == Path(target_path) / test_file.relative_to(resource_path)

                remote_file = PurePosixPath(target_path) / success_file.relative_to(target_path)

                # file size is same as expected
                assert file_connection.get_stat(remote_file).st_size == os.path.getsize(test_file)
                assert file_connection.get_stat(remote_file).st_mtime >= os.path.getmtime(test_file)

                # file content is same as expected
                temp_file = temp_root / success_file.name
                file_connection.download_file(remote_file, temp_file)

                with open(test_file, "rb") as file:
                    assert temp_file.read_bytes() == file.read()

    def test_run_missing_file(self, file_connection, test_files, caplog):
        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

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

        assert upload_result.success
        assert len(upload_result.success) == len(test_files)

        assert upload_result.missing
        assert len(upload_result.missing) == 1
        assert upload_result.missing == {missing_file}

        for missing_file in upload_result.missing:
            assert not missing_file.exists()

    def test_delete_source(self, test_files, file_connection, caplog):
        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
            options=file_connection.Options(delete_source=True),
        )

        with caplog.at_level(logging.WARNING):
            upload_result = uploader.run(test_files)

            assert "LOCAL FILES WILL BE PERMANENTLY DELETED AFTER UPLOADING !!!" in caplog.text

        assert not upload_result.failed
        assert not upload_result.skipped
        assert not upload_result.missing

        assert upload_result.success
        assert len(upload_result.success) == len(test_files)
        assert upload_result.success == {Path(target_path) / file.name for file in test_files}

        # source files are removed
        assert all(not file.exists() for file in test_files)

    @pytest.mark.parametrize(
        "options",
        [{"mode": "error"}, FileConnection.Options(mode="error"), FileConnection.Options(mode=FileWriteMode.ERROR)],
    )
    def test_mode_error(self, file_connection, test_files, options, tmp_path_factory):
        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

        pre_uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
        )

        pre_uploader.run(test_files)  # preload

        # make copy of uploaded files
        new_source = tmp_path_factory.mktemp("test_files_copy")
        for test_file in test_files:
            shutil.copy(test_file, new_source)

        new_files = set(new_source.iterdir())

        for new_file in new_files:
            # change new files content
            new_file.write_bytes(secrets.token_bytes())

            remote_file = target_path / new_file.name
            remote_file_stat = file_connection.get_stat(remote_file)

            # check that file is different now
            assert remote_file_stat.st_size != new_file.stat().st_size

        # upload changed files
        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
            options=options,
        )

        upload_result = uploader.run(new_files)

        assert not upload_result.success
        assert not upload_result.missing
        assert not upload_result.skipped

        assert upload_result.failed
        assert len(upload_result.failed) == len(new_files)
        assert upload_result.failed == set(new_files)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)

            for failed_file in upload_result.failed:
                assert failed_file.exists()
                assert failed_file.is_file()
                assert not failed_file.is_dir()

                assert isinstance(failed_file.exception, FileExistsError)

                remote_file = target_path / failed_file.name
                assert f"Target directory already contains file '{remote_file}'" in str(failed_file.exception)

                # file size wasn't changed
                original_file = next(file for file in test_files if file.name == failed_file.name)
                assert file_connection.get_stat(remote_file).st_size != failed_file.stat().st_size
                assert file_connection.get_stat(remote_file).st_size == original_file.stat().st_size

                # file content wasn't changed
                temp_file = temp_root / failed_file.name
                file_connection.download_file(remote_file, temp_file)
                assert temp_file.read_bytes() == original_file.read_bytes()

    def test_mode_ignore(self, file_connection, test_files, caplog, tmp_path_factory):
        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

        pre_uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
        )

        pre_uploader.run(test_files)  # preload

        # make copy of uploaded files
        new_source = tmp_path_factory.mktemp("test_files_copy")
        for test_file in test_files:
            shutil.copy(test_file, new_source)

        new_files = set(new_source.iterdir())

        target_files = []
        for new_file in new_files:
            # change new files content
            new_file.write_bytes(secrets.token_bytes())

            remote_file = target_path / new_file.name
            remote_file_stat = file_connection.get_stat(remote_file)

            target_files.append(remote_file)

            # check that file is different now
            assert remote_file_stat.st_size != new_file.stat().st_size

        # upload changed files
        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
            options=FileConnection.Options(mode=FileWriteMode.IGNORE),
        )

        with caplog.at_level(logging.WARNING):
            upload_result = uploader.run(new_files)

            for target_file in target_files:
                assert f"Target directory already contains file '{target_file}', skipping" in caplog.text

        assert not upload_result.success
        assert not upload_result.missing
        assert not upload_result.failed

        assert upload_result.skipped
        assert len(upload_result.skipped) == len(new_files)
        assert upload_result.skipped == set(new_files)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)

            for skipped_file in upload_result.skipped:
                assert skipped_file.exists()
                assert skipped_file.is_file()
                assert not skipped_file.is_dir()

                remote_file = target_path / skipped_file.name

                # file size wasn't changed
                original_file = next(file for file in test_files if file.name == skipped_file.name)
                assert file_connection.get_stat(remote_file).st_size != skipped_file.stat().st_size
                assert file_connection.get_stat(remote_file).st_size == original_file.stat().st_size

                # file content wasn't changed
                temp_file = temp_root / skipped_file.name
                file_connection.download_file(remote_file, temp_file)
                assert temp_file.read_bytes() == original_file.read_bytes()

    def test_mode_overwrite(self, file_connection, test_files, caplog, tmp_path_factory):
        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

        pre_uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
        )

        pre_uploader.run(test_files)  # preload

        # make copy of uploaded files
        new_source = tmp_path_factory.mktemp("test_files_copy")
        for test_file in test_files:
            shutil.copy(test_file, new_source)

        new_files = set(new_source.iterdir())

        target_files = []
        for new_file in new_files:
            # change new files content
            new_file.write_bytes(secrets.token_bytes())

            remote_file = target_path / new_file.name
            remote_file_stat = file_connection.get_stat(remote_file)

            target_files.append(remote_file)

            # check that file is different now
            assert remote_file_stat.st_size != new_file.stat().st_size

        # upload changed files
        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
            options=FileConnection.Options(mode=FileWriteMode.OVERWRITE),
        )

        with caplog.at_level(logging.WARNING):
            upload_result = uploader.run(new_files)

            for target_file in target_files:
                assert f"Target directory already contains file '{target_file}', overwriting" in caplog.text

        assert not upload_result.failed
        assert not upload_result.skipped
        assert not upload_result.missing

        assert upload_result.success
        assert len(upload_result.success) == len(new_files)
        assert upload_result.success == {Path(target_path) / file.name for file in new_files}

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)

            for success_file in upload_result.success:
                assert success_file.exists()
                assert success_file.is_file()
                assert not success_file.is_dir()

                changed_file = next(file for file in new_files if file.name == success_file.name)
                remote_file = target_path / success_file.name

                # file size was changed
                original_file = next(file for file in test_files if file.name == success_file.name)
                assert file_connection.get_stat(remote_file).st_size != original_file.stat().st_size
                assert file_connection.get_stat(remote_file).st_size == success_file.stat().st_size
                assert file_connection.get_stat(remote_file).st_size == changed_file.stat().st_size

                # file content was changed
                temp_file = temp_root / success_file.name
                file_connection.download_file(remote_file, temp_file)
                assert temp_file.read_bytes() != original_file.read_bytes()
                assert temp_file.read_bytes() == changed_file.read_bytes()

    def test_mode_delete_all(self, file_connection, test_files, caplog):
        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

        with tempfile.NamedTemporaryFile("w") as file:
            file.write(secrets.token_hex())
            file.flush()

            pre_uploader = FileUploader(
                connection=file_connection,
                target_path=target_path,
            )

            pre_uploader.run(test_files + [file.name])  # upload one more file than needed

        # upload files
        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
            options=FileConnection.Options(mode=FileWriteMode.DELETE_ALL),
        )

        with caplog.at_level(logging.WARNING):
            upload_result = uploader.run(test_files)
            assert "TARGET DIRECTORY WILL BE CLEANED UP BEFORE UPLOADING FILES !!!" in caplog.text

        assert not upload_result.failed
        assert not upload_result.skipped
        assert not upload_result.missing

        assert upload_result.success
        assert len(upload_result.success) == len(test_files)
        assert upload_result.success == {Path(target_path) / test_file.name for test_file in test_files}

    def test_local_path_does_not_exist(self, file_connection, tmp_path_factory):
        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

        local_path_parent = tmp_path_factory.mktemp("local_path")
        local_path = local_path_parent / "abc"

        uploader = FileUploader(connection=file_connection, target_path=target_path, local_path=local_path)

        with pytest.raises(DirectoryNotFoundError, match=f"'{local_path}' does not exist"):
            uploader.run()

    def test_local_path_not_a_directory(self, file_connection):
        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

        with tempfile.NamedTemporaryFile() as file:
            uploader = FileUploader(connection=file_connection, target_path=target_path, local_path=file.name)

            with pytest.raises(NotADirectoryError, match=f"'{file.name}' is not a directory"):
                uploader.run()

    def test_target_path_not_a_directory(self, request, file_connection, tmp_path_factory):
        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
        file_connection.write_text(target_path, "abc")

        def finalizer():
            file_connection.remove_file(target_path)

        request.addfinalizer(finalizer)

        local_path = tmp_path_factory.mktemp("local_path")

        uploader = FileUploader(connection=file_connection, target_path=target_path, local_path=local_path)

        with pytest.raises(NotADirectoryError, match=f"'{target_path}' is not a directory"):
            uploader.run()

    def test_without_files_and_without_local_path(self, file_connection):
        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

        uploader = FileUploader(connection=file_connection, target_path=target_path)

        with pytest.raises(ValueError, match="Neither file collection nor ``local_path`` are passed"):
            uploader.run()

    def test_with_files_and_local_path(self, file_connection, caplog, tmp_path_factory):
        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
        local_path = tmp_path_factory.mktemp("local_path")

        uploader = FileUploader(connection=file_connection, target_path=target_path, local_path=local_path)

        with caplog.at_level(logging.WARNING):
            upload_result = uploader.run([local_path / "path1", local_path / "path2"])
            assert (
                "Passed both ``local_path`` and file collection at the same time. File collection will be used"
            ) in caplog.text

        assert not upload_result.success
        assert not upload_result.failed
        assert not upload_result.skipped
        assert upload_result.missing

    @pytest.mark.parametrize("path_type", [str, PurePosixPath], ids=["path_type str", "path_type Path"])
    def test_with_file_list_relative_path_and_local_path(self, file_connection, resource_path, path_type):
        target_path = path_type(f"/tmp/test_upload_{secrets.token_hex(5)}")

        # upload files
        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
            local_path=resource_path,
        )

        local_relative_path_files_list = []
        for root, _, files in os.walk(resource_path):
            for file_name in files:
                abs_path_file = Path(root) / file_name
                local_relative_path_files_list.append(abs_path_file.relative_to(resource_path))

        upload_result = uploader.run(local_relative_path_files_list)

        assert not upload_result.failed
        assert not upload_result.missing
        assert upload_result.success
        assert sorted(path for path in upload_result.success) == sorted(
            {PurePosixPath(target_path) / file for file in local_relative_path_files_list},
        )  # in order to be able to compare

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)

            for success_file in upload_result.success:
                test_file = next(file for file in local_relative_path_files_list if file.name == success_file.name)
                assert success_file == PurePosixPath(target_path) / test_file

                local_path = resource_path / test_file

                # file size is same as expected
                remote_file = target_path / test_file
                assert file_connection.get_stat(remote_file).st_size == local_path.stat().st_size
                assert file_connection.get_stat(remote_file).st_mtime >= local_path.stat().st_mtime

                # file content is same as expected
                temp_file = temp_root / success_file.name
                file_connection.download_file(remote_file, temp_file)

                with open(resource_path / test_file, "rb") as file:
                    assert temp_file.read_bytes() == file.read()

    def test_path_in_file_list_not_match_local_path_error(self, file_connection, tmp_path_factory):
        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
        local_path = tmp_path_factory.mktemp("local_path")

        uploader = FileUploader(connection=file_connection, target_path=target_path, local_path=local_path)

        with pytest.raises(ValueError, match=f"File path '/some/path/1' does not match source_path '{local_path}'"):
            uploader.run(["/some/path/1", "/some/path/2"])

    def test_run_relative_paths_without_local_path(self, file_connection):
        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")

        uploader = FileUploader(connection=file_connection, target_path=target_path)

        with pytest.raises(ValueError, match="Cannot pass relative file path with empty ``local_path``"):
            uploader.run(["some/path/1", "some/path/2"])

    @pytest.mark.flaky(reruns=5)
    @pytest.mark.parametrize(
        "temp_path, real_temp_path",
        [
            (None, PurePosixPath("/tmp")),
            ("/abc", PurePosixPath("/abc")),
        ],
    )
    def test_generate_temp_path(self, file_connection, tmp_path_factory, temp_path, real_temp_path):
        kwargs = {
            "connection": file_connection,
            "local_path": tmp_path_factory.mktemp("local_path"),
            "target_path": f"/tmp/tmp_{secrets.token_hex()}",
        }
        if temp_path:
            kwargs["temp_path"] = temp_path

        uploader = FileUploader(**kwargs)
        dt_prefix = datetime.now().strftime("%Y%m%d%H%M")  # up to minutes, not seconds

        with Process(name="me", host="currenthost"):
            temp_path = os.fspath(uploader.generate_temp_path())
            expected = os.fspath(real_temp_path / "onetl" / "currenthost" / "me" / dt_prefix)
            assert temp_path.startswith(expected)

        with Process(name="me", host="currenthost", dag="abc", task="cde"):
            temp_path = os.fspath(uploader.generate_temp_path())
            expected = os.fspath(real_temp_path / "onetl" / "currenthost" / "abc.cde.me" / dt_prefix)
            assert temp_path.startswith(expected)

    def test_source_check(self, file_connection, caplog):
        with caplog.at_level(logging.INFO):
            file_connection.check()

        assert "Connection is available" in caplog.text
