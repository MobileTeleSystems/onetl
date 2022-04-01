import tempfile
from pathlib import Path, PurePosixPath

import pytest

from onetl.core import FileDownloader
from tests.lib.common import hashfile


class TestDownloader:
    @pytest.mark.parametrize("path_type", [str, Path])
    def test_delete_source(self, file_connection, source_path, resource_path, upload_test_files, path_type):
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = FileDownloader(
                connection=file_connection,
                source_path=path_type(source_path),
                local_path=path_type(temp_dir),
                delete_source=True,
            )

            downloaded_files = downloader.run()
            local_files = [Path(temp_dir) / file.name for file in upload_test_files]

            assert len(downloaded_files) == len(local_files)
            assert set(downloaded_files) == set(local_files)

            current_ftps_files = set()
            for root, _dirs, downloaded_files in file_connection.walk(source_path):
                root_path = PurePosixPath(root)

                for filename in downloaded_files:
                    current_ftps_files.add(root_path / filename)

            assert not current_ftps_files

            original_files = [resource_path / file.relative_to(source_path) for file in upload_test_files]

            for original_file in original_files:
                assert original_file.stat().st_size == (Path(temp_dir) / original_file.name).stat().st_size
                assert hashfile(original_file) == hashfile(Path(temp_dir) / original_file.name)

    def test_with_wrong_pattern(self, file_connection, source_path, upload_test_files):
        file_pattern = "*.wng"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
                file_pattern=file_pattern,
            )

            downloaded_files = downloader.run()
            assert not downloaded_files

    def test_with_pattern(self, file_connection, source_path, resource_path, upload_test_files):
        file_pattern = "*.csv"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
                file_pattern=file_pattern,
            )

            downloaded_files = downloader.run()

            matching_files = [file for file in upload_test_files if file.match(file_pattern)]
            local_files = [local_path / file.name for file in matching_files]

            assert len(downloaded_files) == len(local_files)
            assert set(downloaded_files) == set(local_files)

            original_files = [
                resource_path / file.relative_to(source_path) for file in upload_test_files if file.match(file_pattern)
            ]
            for original_file in original_files:
                assert original_file.stat().st_size == (local_path / original_file.name).stat().st_size
                assert hashfile(original_file) == hashfile(local_path / original_file.name)

    def test_view_file(self, file_connection, source_path, upload_test_files):
        downloader = FileDownloader(
            connection=file_connection,
            source_path=source_path,
            local_path=Path("/some/path"),
        )

        files_list = downloader.view_files()

        print(files_list)

        for file_path in upload_test_files:
            assert file_path in files_list

    def test_run(self, file_connection, source_path, resource_path, upload_test_files):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
            )

            downloaded_files = downloader.run()
            local_files = [local_path / file.name for file in upload_test_files]

            assert len(downloaded_files) == len(local_files)
            assert set(downloaded_files) == set(local_files)

            original_files = [resource_path / file.relative_to(source_path) for file in upload_test_files]

            for original_file in original_files:
                assert original_file.stat().st_size == (local_path / original_file.name).stat().st_size
                assert hashfile(original_file) == hashfile(local_path / original_file.name)

    def test_empty_dir(self, file_connection, source_path):
        file_pattern = "*.csv"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
                file_pattern=file_pattern,
            )

            downloaded_files = downloader.run()
            assert not downloaded_files
