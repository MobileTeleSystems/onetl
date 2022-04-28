import tempfile
from pathlib import Path, PurePosixPath

import pytest

from onetl.connection import FileConnection
from onetl.core import FileDownloader, FileFilter
from tests.lib.common import hashfile


class TestDownloader:
    @pytest.mark.parametrize("path_type", [str, Path])
    def test_delete_source(self, file_connection, source_path, resource_path, upload_test_files, path_type):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = path_type(temp_dir)
            downloader = FileDownloader(
                connection=file_connection,
                source_path=path_type(source_path),
                local_path=local_path,
                options=file_connection.Options(delete_source=True),
            )

            downloaded_files = downloader.run()

            local_files = [local_path / file.relative_to(source_path) for file in upload_test_files]

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
                local_file = local_path / original_file.relative_to(resource_path)
                assert original_file.stat().st_size == local_file.stat().st_size
                assert hashfile(original_file) == hashfile(local_file)

    def test_with_wrong_pattern(self, file_connection, source_path, upload_test_files):
        file_pattern = "*.wng"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
                filter=FileFilter(glob=file_pattern),
            )

            downloaded_files = downloader.run()
            assert not downloaded_files

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_with_filter_exclude_dir(self, file_connection, source_path, resource_path, upload_test_files, path_type):
        exclude_dir = path_type("/export/news_parse/exclude_dir")

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
                filter=FileFilter(exclude_dirs=[exclude_dir]),
            )

            downloaded_files = downloader.run()

            matching_files = [file for file in upload_test_files if PurePosixPath(exclude_dir) not in file.parents]

            local_files = [local_path / file.relative_to(source_path) for file in matching_files]

            assert len(downloaded_files) == len(local_files)
            assert set(downloaded_files) == set(local_files)

            original_files = [resource_path / file.relative_to(source_path) for file in matching_files]

            for original_file in original_files:
                local_file = local_path / original_file.relative_to(resource_path)
                assert original_file.stat().st_size == local_file.stat().st_size
                assert hashfile(original_file) == hashfile(local_file)

    def test_with_filter_glob(self, file_connection, source_path, resource_path, upload_test_files):
        file_pattern = "*.csv"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
                filter=FileFilter(glob=file_pattern),
            )

            downloaded_files = downloader.run()

            matching_files = [file for file in upload_test_files if file.match(file_pattern)]
            local_files = [local_path / file.relative_to(source_path) for file in matching_files]

            assert len(downloaded_files) == len(local_files)
            assert set(downloaded_files) == set(local_files)

            original_files = [
                resource_path / file.relative_to(source_path) for file in upload_test_files if file.match(file_pattern)
            ]
            for original_file in original_files:
                local_file = local_path / original_file.relative_to(resource_path)
                assert original_file.stat().st_size == local_file.stat().st_size
                assert hashfile(original_file) == hashfile(local_file)

    def test_view_file(self, file_connection, source_path, upload_test_files):
        downloader = FileDownloader(
            connection=file_connection,
            source_path=source_path,
            local_path=Path("/some/path"),
        )

        files_list = downloader.view_files()

        assert files_list
        assert len(files_list) == len(upload_test_files)
        assert set(files_list) == set(upload_test_files)

    def test_run(self, file_connection, source_path, resource_path, upload_test_files):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
            )

            downloaded_files = downloader.run()
            local_files = [local_path / file.relative_to(source_path) for file in upload_test_files]

            assert len(downloaded_files) == len(local_files)
            assert set(downloaded_files) == set(local_files)

            original_files = [resource_path / file.relative_to(source_path) for file in upload_test_files]

            for original_file in original_files:
                local_file = local_path / original_file.relative_to(resource_path)
                assert original_file.stat().st_size == local_file.stat().st_size
                assert hashfile(original_file) == hashfile(local_file)

    @pytest.mark.parametrize(
        "source_path_param",
        [None, "/export/news_parse"],
        ids=["Without source_path", "With source path"],
    )
    def test_run_with_files_list_absolute(
        self,
        file_connection,
        source_path,
        resource_path,
        upload_test_files,
        source_path_param,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path_param,
                local_path=local_path,
            )

            downloaded_files = downloader.run(files_list=upload_test_files)

            if source_path_param:
                local_files = [local_path / file.relative_to(source_path_param) for file in upload_test_files]

            else:
                local_files = [local_path / file.name for file in upload_test_files]

            assert len(downloaded_files) == len(local_files)
            assert set(downloaded_files) == set(local_files)

            original_files = [resource_path / file.relative_to(source_path) for file in upload_test_files]

            for original_file in original_files:
                if not source_path_param:
                    assert original_file.stat().st_size == (local_path / original_file.name).stat().st_size
                    assert hashfile(original_file) == hashfile(local_path / original_file.name)
                if source_path_param:
                    local_file = local_path / original_file.relative_to(resource_path)
                    assert original_file.stat().st_size == local_file.stat().st_size
                    assert hashfile(original_file) == hashfile(local_file)

    def test_run_with_files_list_relative(
        self,
        file_connection,
        source_path,
        resource_path,
        upload_test_files,
    ):
        relative_files_path = [file.relative_to(source_path) for file in upload_test_files]

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
            )

            downloaded_files = downloader.run(files_list=relative_files_path)

            local_files = [local_path / file.relative_to(source_path) for file in upload_test_files]

            assert len(downloaded_files) == len(local_files)
            assert set(downloaded_files) == set(local_files)

            original_files = [resource_path / file.relative_to(source_path) for file in upload_test_files]

            for original_file in original_files:
                local_file = local_path / original_file.relative_to(resource_path)
                assert original_file.stat().st_size == local_file.stat().st_size
                assert hashfile(original_file) == hashfile(local_file)

    def test_run_without_source_path_and_file_list_fail(
        self,
        file_connection,
        source_path,
        resource_path,
        upload_test_files,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                local_path=local_path,
            )

            with pytest.raises(ValueError):
                downloader.run()

    def test_run_without_source_path_with_empty_files_list(
        self,
        file_connection,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                local_path=local_path,
            )

            downloaded_files = downloader.run([])

            assert not len(downloaded_files)
            assert set(downloaded_files) == set()

    def test_run_with_empty_files_list(
        self,
        file_connection,
        source_path,
        resource_path,
        upload_test_files,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                local_path=local_path,
                source_path=source_path,
            )

            downloaded_files = downloader.run([])

            assert not len(downloaded_files)
            assert set(downloaded_files) == set()

    def test_run_relative_path_without_source_path(
        self,
        file_connection,
        source_path,
        resource_path,
        upload_test_files,
    ):
        relative_files_path = [*upload_test_files, PurePosixPath("some/relative/path/file.txt")]

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                local_path=local_path,
            )

            with pytest.raises(ValueError):
                downloader.run(relative_files_path)

    def test_run_absolute_path_not_match_with_source_path(
        self,
        file_connection,
        source_path,
        resource_path,
        upload_test_files,
    ):
        relative_files_path = [*upload_test_files, PurePosixPath("/some/relative/path/file.txt")]

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
            )

            with pytest.raises(ValueError):
                downloader.run(relative_files_path)

    def test_empty_dir(self, file_connection, source_path):
        file_pattern = "*.csv"

        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
                filter=FileFilter(glob=file_pattern),
            )

            downloaded_files = downloader.run()
            assert not downloaded_files

    @pytest.mark.parametrize("options", [dict, FileConnection.Options])
    def test_download_mode_error(self, file_connection, source_path, resource_path, upload_test_files, options):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            pre_downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
            )

            pre_downloader.run()  # preload

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
                options=options(mode="error"),
            )

            with pytest.raises(FileExistsError):
                downloader.run()

    @pytest.mark.parametrize("options", [dict, FileConnection.Options])
    def test_download_mode_ignore(self, file_connection, source_path, resource_path, upload_test_files, options):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            pre_downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
            )

            pre_downloader.run()  # preload

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
                options=options(mode="ignore"),
            )

            downloaded_files = downloader.run()

            assert not len(downloaded_files)
            assert set(downloaded_files) == set()

    @pytest.mark.parametrize("options", [dict, FileConnection.Options])
    def test_download_mode_overwrite(self, file_connection, source_path, resource_path, upload_test_files, options):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            pre_downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
            )

            pre_downloader.run()  # preload

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
                options=options(mode="overwrite"),
            )

            with tempfile.NamedTemporaryFile(mode="w+") as temp_file:
                temp_file.write("overwrited")
                temp_file.flush()

                # remove from source old file
                file_connection.remove_file(source_path / "news_parse_zp" / "exclude_dir" / "file_1.txt")

                # Upload to source overwrited file
                file_connection.upload_file(
                    local_file_path=Path("tmp") / temp_file.name,
                    remote_file_path=source_path / "news_parse_zp" / "exclude_dir" / "file_1.txt",
                )

            downloaded_files = downloader.run()

            # find path to overwrited file
            for file in downloaded_files:
                if file.name == "file_1.txt":
                    overwrited_file_path = file

            with open(overwrited_file_path) as temp_file:
                assert temp_file.read() == "overwrited"

            local_files = [local_path / local.relative_to(source_path) for local in upload_test_files]

            assert len(downloaded_files) == len(local_files)
            assert set(downloaded_files) == set(local_files)

    @pytest.mark.parametrize("options", [dict, FileConnection.Options])
    def test_download_mode_delete_all(self, file_connection, source_path, resource_path, upload_test_files, options):
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path = Path(temp_dir)

            pre_downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
            )

            pre_downloader.run()  # preload

            downloader = FileDownloader(
                connection=file_connection,
                source_path=source_path,
                local_path=local_path,
                options=options(mode="delete_all"),
            )

            (local_path / "file_9.txt").touch()

            downloaded_files = downloader.run()

            local_files = [local_path / file.relative_to(source_path) for file in upload_test_files]

            assert len(downloaded_files) == len(local_files)
            assert set(downloaded_files) == set(local_files)
