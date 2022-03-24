import logging
import tempfile
from pathlib import Path, PurePosixPath
import secrets

import pytest

from onetl.connection import SFTP
from onetl.core import FileDownloader, FileUploader
from tests.lib.common import hashfile


class TestSFTP:
    def test_sftp_source_check(self, sftp_server, caplog):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.password, host=sftp_server.host, port=sftp_server.port)

        with caplog.at_level(logging.INFO):
            sftp.check()

        assert "Connection is available" in caplog.text

    def test_sftp_wrong_source_check(self):
        sftp = SFTP(user="some_user", password="pwd", host="host", port=123)

        with pytest.raises(RuntimeError):
            sftp.check()

    def test_sftp_file_uploader_with_empty_file_list(self, sftp_server, caplog):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.password, host=sftp_server.host, port=sftp_server.port)

        uploader = FileUploader(connection=sftp, target_path="/target/path/")

        with caplog.at_level(logging.INFO):
            uploaded_files = uploader.run([])
            assert "Files list is empty. Please, provide files to upload." in caplog.text

        assert not uploaded_files

    def test_sftp_file_uploader(self, sftp_server, test_files):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.password, host=sftp_server.host, port=sftp_server.port)

        target_path = PurePosixPath("/tmp/test_upload")
        uploader = FileUploader(connection=sftp, target_path=target_path)
        uploaded_files = uploader.run(test_files)

        assert uploaded_files == [target_path / test_file.name for test_file in test_files]

    def test_sftp_file_uploader_delete_source(self, make_test_files_copy, sftp_server):

        sftp = SFTP(user=sftp_server.user, password=sftp_server.password, host=sftp_server.host, port=sftp_server.port)

        target_path = PurePosixPath(f"/tmp/test_upload_{secrets.token_hex(5)}")
        uploader = FileUploader(connection=sftp, target_path=target_path, delete_local=True)
        uploader.run(make_test_files_copy)

        # Check out the source folder. The folder must be empty.
        for file in make_test_files_copy:
            assert not file.is_file()

    def test_sftp_file_downloader_empty_dir(self, sftp_server, source_path):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.password, host=sftp_server.host, port=sftp_server.port)
        file_pattern = "*.csv"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=sftp,
                source_path=source_path,
                local_path=local_path,
                file_pattern=file_pattern,
            )

            files = downloader.run()
            assert not files

    def test_sftp_file_downloader(self, sftp_server, source_path, resource_path, sftp_files):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.password, host=sftp_server.host, port=sftp_server.port)

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=sftp,
                source_path=source_path,
                local_path=local_path,
            )

            files = downloader.run()
            local_files = [local_path / file.name for file in sftp_files]

            assert len(files) == len(local_files)
            assert set(files) == set(local_files)

            original_files = [resource_path / file.relative_to(source_path) for file in sftp_files]

            for original_file in original_files:
                assert original_file.stat().st_size == (local_path / original_file.name).stat().st_size
                assert hashfile(original_file) == hashfile(local_path / original_file.name)

    def test_sftp_file_downloader_with_pattern(self, sftp_server, source_path, resource_path, sftp_files):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.password, host=sftp_server.host, port=sftp_server.port)
        file_pattern = "*.csv"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=sftp,
                source_path=source_path,
                local_path=local_path,
                file_pattern=file_pattern,
            )

            files = downloader.run()

            matching_files = [file for file in sftp_files if file.match(file_pattern)]
            local_files = [local_path / file.name for file in matching_files]

            assert len(files) == len(local_files)
            assert set(files) == set(local_files)

            original_files = [
                resource_path / file.relative_to(source_path) for file in sftp_files if file.match(file_pattern)
            ]
            for original_file in original_files:
                assert original_file.stat().st_size == (local_path / original_file.name).stat().st_size
                assert hashfile(original_file) == hashfile(local_path / original_file.name)

    def test_sftp_file_downloader_with_wrong_pattern(self, sftp_server, source_path, sftp_files):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.password, host=sftp_server.host, port=sftp_server.port)
        file_pattern = "*.wng"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=sftp,
                source_path=source_path,
                local_path=local_path,
                file_pattern=file_pattern,
            )

            files = downloader.run()
            assert not files

    def test_sftp_file_downloader_delete_source(self, sftp_server, source_path, resource_path, sftp_files):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=sftp,
                source_path=source_path,
                local_path=local_path,
                delete_source=True,
            )

            files = downloader.run()
            local_files = [local_path / file.name for file in sftp_files]

            assert len(files) == len(local_files)
            assert set(files) == set(local_files)

            current_sftp_files = set()
            for root, _dirs, files in sftp.walk(source_path):
                root_path = PurePosixPath(root)

                for filename in files:
                    current_sftp_files.add(root_path / filename)

            assert not current_sftp_files

            original_files = [resource_path / file.relative_to(source_path) for file in sftp_files]

            for original_file in original_files:
                assert original_file.stat().st_size == (local_path / original_file.name).stat().st_size
                assert hashfile(original_file) == hashfile(local_path / original_file.name)
