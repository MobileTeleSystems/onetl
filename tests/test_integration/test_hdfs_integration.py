import logging
import tempfile
from pathlib import Path, PurePosixPath

import pytest

from onetl.connection import HDFS
from onetl.core import FileDownloader, FileUploader
from tests.lib.common import hashfile


class TestHDFS:
    def test_hdfs_source_check(self, hdfs_server, caplog):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port)

        with caplog.at_level(logging.INFO):
            hdfs.check()

        assert "Connection is available" in caplog.text

    def test_hdfs_wrong_source_check(self):
        hdfs = HDFS(host="hive1", port=1234)

        with pytest.raises(RuntimeError):
            hdfs.check()

    def test_hdfs_file_uploader_with_empty_file_list(self, hdfs_server, caplog):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port)

        uploader = FileUploader(connection=hdfs, target_path="/target/path/")

        with caplog.at_level(logging.INFO):
            uploaded_files = uploader.run([])
            assert "Files list is empty. Please, provide files to upload." in caplog.text

        assert not uploaded_files

    def test_hdfs_file_uploader(self, hdfs_server, test_files):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port)

        target_path = PurePosixPath("/user/onetl/test_upload")
        uploader = FileUploader(connection=hdfs, target_path=target_path)
        uploaded_files = uploader.run(test_files)

        assert uploaded_files == [target_path / test_file.name for test_file in test_files]

    def test_hdfs_file_downloader_empty_dir(self, hdfs_server, source_path):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port)
        file_pattern = "*.csv"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=hdfs,
                source_path=source_path,
                local_path=local_path,
                source_file_pattern=file_pattern,
            )

            files = downloader.run()
            assert not files

    def test_hdfs_file_downloader(self, hdfs_server, source_path, resource_path, hdfs_files):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port)

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=hdfs,
                source_path=source_path,
                local_path=local_path,
            )

            files = downloader.run()
            local_files = [local_path / file.name for file in hdfs_files]

            assert len(files) == len(local_files)
            assert set(files) == set(local_files)

            original_files = [resource_path / file.relative_to(source_path) for file in hdfs_files]

            for original_file in original_files:
                assert original_file.stat().st_size == (local_path / original_file.name).stat().st_size
                assert hashfile(original_file) == hashfile(local_path / original_file.name)

    def test_hdfs_file_downloader_with_pattern(self, hdfs_server, source_path, resource_path, hdfs_files):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port)
        file_pattern = "*.csv"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=hdfs,
                source_path=source_path,
                local_path=local_path,
                source_file_pattern=file_pattern,
            )

            files = downloader.run()

            matching_files = [file for file in hdfs_files if file.match(file_pattern)]
            local_files = [local_path / file.name for file in matching_files]

            assert len(files) == len(local_files)
            assert set(files) == set(local_files)

            original_files = [
                resource_path / file.relative_to(source_path) for file in hdfs_files if file.match(file_pattern)
            ]
            for original_file in original_files:
                assert original_file.stat().st_size == (local_path / original_file.name).stat().st_size
                assert hashfile(original_file) == hashfile(local_path / original_file.name)

    def test_hdfs_file_downloader_with_wrong_pattern(self, hdfs_server, source_path, hdfs_files):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port)
        file_pattern = "*.wng"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=hdfs,
                source_path=source_path,
                local_path=local_path,
                source_file_pattern=file_pattern,
            )

            files = downloader.run()
            assert not files

    def test_hdfs_file_downloader_delete_source(self, hdfs_server, source_path, resource_path, hdfs_files):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port)

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=hdfs,
                source_path=source_path,
                local_path=local_path,
                delete_source=True,
            )

            files = downloader.run()
            local_files = [local_path / file.name for file in hdfs_files]

            assert len(files) == len(local_files)
            assert set(files) == set(local_files)

            current_hdfs_files = set()
            for root, _dirs, files in hdfs.walk(source_path):
                root_path = PurePosixPath(root)

                for filename in files:
                    current_hdfs_files.add(root_path / filename)

            assert not current_hdfs_files

            original_files = [resource_path / file.relative_to(source_path) for file in hdfs_files]

            for original_file in original_files:
                assert original_file.stat().st_size == (local_path / original_file.name).stat().st_size
                assert hashfile(original_file) == hashfile(local_path / original_file.name)
