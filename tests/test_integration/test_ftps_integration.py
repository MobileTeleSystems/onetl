import logging
import os
import posixpath
import tempfile
from pathlib import PosixPath

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

    def test_ftps_downloader_with_pattern(self, ftps_server, ftps_files, source_path, test_file_name, test_file_path):
        ftp = FTPS(user=ftps_server.user, password=ftps_server.password, host=ftps_server.host, port=ftps_server.port)
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

    def test_ftps_downloader_delete_source(self, ftps_files, ftps_server, source_path):
        ftp = FTPS(user=ftps_server.user, password=ftps_server.password, host=ftps_server.host, port=ftps_server.port)
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

    def test_ftps_uploader(self, ftps_server, test_file_name, test_file_path):
        ftp = FTPS(user=ftps_server.user, password=ftps_server.password, host=ftps_server.host, port=ftps_server.port)

        uploader = FileUploader(connection=ftp, target_path="/tmp/test_upload")
        files = [
            test_file_path,
        ]

        uploaded_files = uploader.run(files)
        assert uploaded_files == [PosixPath("/tmp/test_upload") / test_file_name]
