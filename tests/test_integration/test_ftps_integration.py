import logging
import tempfile
from pathlib import Path, PurePosixPath
import secrets

import pytest

from onetl.connection import FTPS
from onetl.core import FileDownloader, FileUploader
from tests.lib.common import hashfile


class TestFTPS:
    def test_ftps_source_check(self, ftps_server, caplog):
        ftps = FTPS(user=ftps_server.user, password=ftps_server.password, host=ftps_server.host, port=ftps_server.port)

        with caplog.at_level(logging.INFO):
            ftps.check()

        assert "Connection is available" in caplog.text

    def test_ftps_wrong_source_check(self):
        ftps = FTPS(user="some_user", password="pwd", host="host", port=123)

        with pytest.raises(RuntimeError):
            ftps.check()

    def test_ftps_file_uploader_with_empty_file_list(self, ftps_server, caplog):
        ftps = FTPS(user=ftps_server.user, password=ftps_server.password, host=ftps_server.host, port=ftps_server.port)

        uploader = FileUploader(connection=ftps, target_path="/target/path/")

        with caplog.at_level(logging.INFO):
            uploaded_files = uploader.run([])
            assert "Files list is empty. Please, provide files to upload." in caplog.text

        assert not uploaded_files

    def test_ftps_file_uploader(self, ftps_server, test_files):
        ftps = FTPS(user=ftps_server.user, password=ftps_server.password, host=ftps_server.host, port=ftps_server.port)

        target_path = PurePosixPath("/tmp/test_upload")
        uploader = FileUploader(connection=ftps, target_path=target_path)
        uploaded_files = uploader.run(test_files)

        assert uploaded_files == [target_path / test_file.name for test_file in test_files]

    def test_ftps_file_uploader_delete_source(self, make_test_files_copy, ftps_server):

        ftps = FTPS(user=ftps_server.user, password=ftps_server.password, host=ftps_server.host, port=ftps_server.port)

        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
        uploader = FileUploader(connection=ftps, target_path=target_path, delete_local=True)
        uploader.run(make_test_files_copy)

        # Check out the source folder. The folder must be empty.
        for file in make_test_files_copy:
            assert not file.is_file()

    def test_ftps_file_downloader_empty_dir(self, ftps_server, source_path):
        ftps = FTPS(user=ftps_server.user, password=ftps_server.password, host=ftps_server.host, port=ftps_server.port)
        file_pattern = "*.csv"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=ftps,
                source_path=source_path,
                local_path=local_path,
                file_pattern=file_pattern,
            )

            files = downloader.run()
            assert not files

    def test_ftps_file_downloader(self, ftps_server, source_path, resource_path, ftps_files):
        ftps = FTPS(user=ftps_server.user, password=ftps_server.password, host=ftps_server.host, port=ftps_server.port)

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=ftps,
                source_path=source_path,
                local_path=local_path,
            )

            files = downloader.run()
            local_files = [local_path / file.name for file in ftps_files]

            assert len(files) == len(local_files)
            assert set(files) == set(local_files)

            original_files = [resource_path / file.relative_to(source_path) for file in ftps_files]

            for original_file in original_files:
                assert original_file.stat().st_size == (local_path / original_file.name).stat().st_size
                assert hashfile(original_file) == hashfile(local_path / original_file.name)

    def test_ftps_file_downloader_with_pattern(self, ftps_server, source_path, resource_path, ftps_files):
        ftps = FTPS(user=ftps_server.user, password=ftps_server.password, host=ftps_server.host, port=ftps_server.port)
        file_pattern = "*.csv"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=ftps,
                source_path=source_path,
                local_path=local_path,
                file_pattern=file_pattern,
            )

            files = downloader.run()

            matching_files = [file for file in ftps_files if file.match(file_pattern)]
            local_files = [local_path / file.name for file in matching_files]

            assert len(files) == len(local_files)
            assert set(files) == set(local_files)

            original_files = [
                resource_path / file.relative_to(source_path) for file in ftps_files if file.match(file_pattern)
            ]
            for original_file in original_files:
                assert original_file.stat().st_size == (local_path / original_file.name).stat().st_size
                assert hashfile(original_file) == hashfile(local_path / original_file.name)

    def test_ftps_file_downloader_with_wrong_pattern(self, ftps_server, source_path, ftps_files):
        ftps = FTPS(user=ftps_server.user, password=ftps_server.password, host=ftps_server.host, port=ftps_server.port)
        file_pattern = "*.wng"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=ftps,
                source_path=source_path,
                local_path=local_path,
                file_pattern=file_pattern,
            )

            files = downloader.run()
            assert not files

    def test_ftps_file_downloader_delete_source(self, ftps_server, source_path, resource_path, ftps_files):
        ftps = FTPS(user=ftps_server.user, password=ftps_server.user, host=ftps_server.host, port=ftps_server.port)

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=ftps,
                source_path=source_path,
                local_path=local_path,
                delete_source=True,
            )

            files = downloader.run()
            local_files = [local_path / file.name for file in ftps_files]

            assert len(files) == len(local_files)
            assert set(files) == set(local_files)

            current_ftps_files = set()
            for root, _dirs, files in ftps.walk(source_path):
                root_path = PurePosixPath(root)

                for filename in files:
                    current_ftps_files.add(root_path / filename)

            assert not current_ftps_files

            original_files = [resource_path / file.relative_to(source_path) for file in ftps_files]

            for original_file in original_files:
                assert original_file.stat().st_size == (local_path / original_file.name).stat().st_size
                assert hashfile(original_file) == hashfile(local_path / original_file.name)
