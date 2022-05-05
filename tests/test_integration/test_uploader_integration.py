import logging
import secrets
import shutil
import tempfile
from pathlib import Path, PurePosixPath

import pytest

from onetl.connection import FileConnection
from onetl.core import FileUploader


class TestUploader:
    @pytest.mark.parametrize("path_type", [str, PurePosixPath], ids=["path_type str", "path_type Path"])
    @pytest.mark.parametrize(
        "run_path_type",
        [str, PurePosixPath],
        ids=["run_path_type str", "run_path_type PurePosixPath"],
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

    def test_run_missing_file(self, file_connection, test_files):
        target_path = Path(f"/tmp/test_upload_{secrets.token_hex(5)}")

        # upload files
        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
        )

        missing_file = Path(f"/tmp/test_upload_{secrets.token_hex(5)}")
        upload_result = uploader.run(test_files + [missing_file])

        assert not upload_result.failed
        assert not upload_result.skipped

        assert upload_result.success
        assert len(upload_result.success) == len(test_files)

        assert upload_result.missing
        assert len(upload_result.missing) == 1
        assert upload_result.missing == {missing_file}

        for missing_file in upload_result.missing:
            assert not missing_file.exists()

    def test_delete_source(self, test_files, file_connection):
        target_path = Path(f"/tmp/test_upload_{secrets.token_hex(5)}")

        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
            options=file_connection.Options(delete_source=True),
        )

        upload_result = uploader.run(test_files)

        assert not upload_result.failed
        assert not upload_result.skipped
        assert not upload_result.missing

        assert upload_result.success
        assert len(upload_result.success) == len(test_files)
        assert upload_result.success == {Path(target_path) / file.name for file in test_files}

        # source files are removed
        assert all(not file.exists() for file in test_files)

    @pytest.mark.parametrize("options", [dict, FileConnection.Options])
    def test_upload_options_error(self, file_connection, test_files, options, tmp_path_factory):
        target_path = Path(f"/tmp/test_upload_{secrets.token_hex(5)}")

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
            options=options(mode="error"),
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

    @pytest.mark.parametrize("options", [dict, FileConnection.Options])
    def test_upload_options_ignore(self, file_connection, test_files, options, tmp_path_factory):
        target_path = Path(f"/tmp/test_upload_{secrets.token_hex(5)}")

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
            options=options(mode="ignore"),
        )

        upload_result = uploader.run(new_files)

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

    @pytest.mark.parametrize("options", [dict, FileConnection.Options])
    def test_upload_options_overwrite(self, file_connection, test_files, options, tmp_path_factory):
        target_path = Path(f"/tmp/test_upload_{secrets.token_hex(5)}")

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
            options=options(mode="overwrite"),
        )

        upload_result = uploader.run(new_files)

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

    @pytest.mark.parametrize("options", [dict, FileConnection.Options])
    def test_upload_options_delete_all(self, file_connection, test_files, options):
        target_path = Path(f"/tmp/test_upload_{secrets.token_hex(5)}")

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
            options=options(mode="delete_all"),
        )

        upload_result = uploader.run(test_files)

        assert not upload_result.failed
        assert not upload_result.skipped
        assert not upload_result.missing

        assert upload_result.success
        assert len(upload_result.success) == len(test_files)
        assert upload_result.success == {Path(target_path) / test_file.name for test_file in test_files}

    def test_with_empty_file_list(self, file_connection, caplog):
        uploader = FileUploader(connection=file_connection, target_path="/target/path/")

        with caplog.at_level(logging.INFO):
            upload_result = uploader.run([])
            assert "Files list is empty. Please, provide files to upload." in caplog.text

        assert not upload_result.success
        assert not upload_result.failed
        assert not upload_result.skipped
        assert not upload_result.missing

    def test_source_check(self, file_connection, caplog):
        with caplog.at_level(logging.INFO):
            file_connection.check()

        assert "Connection is available" in caplog.text
