import logging
from pathlib import Path, PurePosixPath
import secrets

import pytest

from onetl.core import FileUploader


class TestUploader:
    @pytest.mark.parametrize("path_type", [str, Path])
    @pytest.mark.parametrize(
        "run_path_type",
        [str, PurePosixPath],
        ids=["run_path_type str", "run_path_type PurePosixPath"],
    )
    def test_delete_source(self, make_test_files_copy, file_connection, path_type, run_path_type):
        target_path = path_type(f"/tmp/test_upload_{secrets.token_hex(5)}")
        uploader = FileUploader(connection=file_connection, target_path=target_path, delete_local=True)
        uploader.run([run_path_type(file) for file in make_test_files_copy])

        # Check out the source folder. The folder must be empty.
        for file in make_test_files_copy:
            assert not file.is_file()

    @pytest.mark.parametrize("path_type", [str, Path], ids=["path_type str", "path_type Path"])
    @pytest.mark.parametrize(
        "run_path_type",
        [str, PurePosixPath],
        ids=["run_path_type str", "run_path_type PurePosixPath"],
    )
    def test_run(self, file_connection, make_test_files_copy, run_path_type, path_type):

        target_path = path_type(f"/tmp/test_upload_{secrets.token_hex(5)}")
        uploader = FileUploader(connection=file_connection, target_path=target_path)
        uploaded_files = uploader.run([run_path_type(file) for file in make_test_files_copy])

        assert uploaded_files == [Path(target_path) / test_file.name for test_file in make_test_files_copy]

    def test_with_empty_file_list(self, file_connection, caplog):

        uploader = FileUploader(connection=file_connection, target_path="/target/path/")

        with caplog.at_level(logging.INFO):
            uploaded_files = uploader.run([])
            assert "Files list is empty. Please, provide files to upload." in caplog.text

        assert not uploaded_files

    def test_source_check(self, file_connection, caplog):

        with caplog.at_level(logging.INFO):
            file_connection.check()

        assert "Connection is available" in caplog.text
