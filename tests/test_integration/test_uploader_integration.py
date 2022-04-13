import logging
from pathlib import Path, PurePosixPath
import secrets
import tempfile

import pytest

from onetl.core import FileUploader
from onetl.connection.file_connection.file_connection import FileConnection


class TestUploader:
    @pytest.mark.parametrize("path_type", [str, Path])
    @pytest.mark.parametrize(
        "run_path_type",
        [str, PurePosixPath],
        ids=["run_path_type str", "run_path_type PurePosixPath"],
    )
    def test_delete_source(self, make_test_files_copy, file_connection, path_type, run_path_type):
        target_path = path_type(f"/tmp/test_upload_{secrets.token_hex(5)}")
        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
            options=file_connection.Options(delete_source=True),
        )
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

    @pytest.mark.parametrize("options", [dict, FileConnection.Options])
    def test_upload_options_error(self, file_connection, make_test_files_copy, options):
        target_path = Path(f"/tmp/test_upload_{secrets.token_hex(5)}")

        pre_uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
        )

        pre_uploader.run(make_test_files_copy)  # preload

        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
            options=options(mode="error"),
        )

        with pytest.raises(RuntimeError):
            uploader.run(make_test_files_copy)

    @pytest.mark.parametrize("options", [dict, FileConnection.Options])
    def test_upload_options_ignore(self, file_connection, make_test_files_copy, options):
        target_path = Path(f"/tmp/test_upload_{secrets.token_hex(5)}")

        pre_uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
        )

        pre_uploader.run(make_test_files_copy)  # preload

        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
            options=options(mode="ignore"),
        )

        uploaded_files = uploader.run(make_test_files_copy)

        assert not uploaded_files
        assert set(uploaded_files) == set()

    @pytest.mark.parametrize("options", [dict, FileConnection.Options])
    def test_upload_options_overwrite(self, file_connection, make_test_files_copy, options):
        target_path = Path(f"/tmp/test_upload_{secrets.token_hex(5)}")

        pre_uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
        )

        pre_uploader.run(make_test_files_copy)  # preload

        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
            options=options(mode="overwrite"),
        )

        with open(make_test_files_copy[0], "w") as overwrited_file:
            overwrited_file.write("overwrited")

        uploaded_files = uploader.run(make_test_files_copy)

        with tempfile.TemporaryDirectory() as temp_dir:
            downloaded_file = Path(temp_dir) / Path("from_ftp.csv")
            file_connection.download_file(
                remote_file_path=target_path / make_test_files_copy[0].name,  # noqa: WPS441
                local_file_path=downloaded_file,
            )

            with open(downloaded_file, "r") as temp_file:
                string = temp_file.read()
                assert string == "overwrited"

        assert uploaded_files == [Path(target_path) / test_file.name for test_file in make_test_files_copy]

    @pytest.mark.parametrize("options", [dict, FileConnection.Options])
    def test_upload_options_delete_all(self, file_connection, make_test_files_copy, options):
        target_path = Path(f"/tmp/test_upload_{secrets.token_hex(5)}")

        pre_uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
        )

        with open(
            PurePosixPath("/tmp/pytest-of-onetl/pytest-0/tmp_test_files0/file_9.txt"),
            "w+",
        ) as temp_file:  # noqa: WPS328, E501
            # preload
            pre_uploader.run(make_test_files_copy + [temp_file.name])

        uploader = FileUploader(
            connection=file_connection,
            target_path=target_path,
            options=options(mode="delete_all"),
        )

        uploaded_files = uploader.run(make_test_files_copy)

        assert uploaded_files == [Path(target_path) / test_file.name for test_file in make_test_files_copy]
        assert len(file_connection.listdir(target_path)) == len(uploaded_files)

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
