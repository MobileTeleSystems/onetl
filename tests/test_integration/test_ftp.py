import posixpath
import logging
from pathlib import PosixPath
import os
import tempfile

from onetl.connection.file_connection import FTP
from onetl.downloader import FileDownloader
from onetl.uploader import FileUploader
from tests.lib.common import hashfile


class TestFTP:
    def test_ftp_source_check(self, ftp_server, caplog):
        ftp = FTP(user=ftp_server.user, password=ftp_server.password, host=ftp_server.host, port=ftp_server.port)
        with caplog.at_level(logging.INFO):
            ftp.check()
        assert "Connection is available" in caplog.text

    def test_ftp_downloader_with_pattern(self, ftp_server, ftp_files, source_path, test_file_name, test_file_path):
        ftp = FTP(user=ftp_server.user, password=ftp_server.password, host=ftp_server.host, port=ftp_server.port)
        with tempfile.TemporaryDirectory() as local_path:
            downloader = FileDownloader(
                connection=ftp,
                source_path=source_path,
                local_path=local_path,
                source_file_pattern="*.csv",
            )

            files = downloader.run()
            # file list comparison
            assert files == [PosixPath(local_path) / test_file_name]
            # compare size of files
            assert os.path.getsize(test_file_path) == os.path.getsize(os.path.join(local_path, test_file_name))
            # compare files
            assert hashfile(test_file_path) == hashfile(os.path.join(local_path, test_file_name))

    def test_ftp_downloader_delete_source(self, ftp_files, ftp_server, source_path):
        ftp = FTP(user=ftp_server.user, password=ftp_server.user, host=ftp_server.host, port=ftp_server.port)
        with tempfile.TemporaryDirectory() as local_path:

            downloader = FileDownloader(
                connection=ftp,
                source_path=source_path,
                local_path=local_path,
                delete_source=True,
            )

            downloaded_files = downloader.run()

            current_ftp_files = set()
            for root, _dirs, files in ftp.client.walk(source_path):
                for filename in files:
                    current_ftp_files.add(posixpath.join(root, filename))

            assert downloaded_files
            assert not current_ftp_files

    def test_ftp_uploader(self, ftp_server, test_file_name, test_file_path):
        ftp = FTP(user=ftp_server.user, password=ftp_server.password, host=ftp_server.host, port=ftp_server.port)

        uploader = FileUploader(connection=ftp, target_path="/tmp/test_upload")
        files = [
            test_file_path,
        ]

        uploaded_files = uploader.run(files)
        assert uploaded_files == [PosixPath("/tmp/test_upload") / test_file_name]
