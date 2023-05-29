import contextlib
import secrets

from etl_entities import FileListHWM, RemoteFolder
from etl_entities.instance import RelativePath

from onetl.file import FileDownloader
from onetl.hwm.store import YAMLHWMStore
from onetl.strategy import IncrementalStrategy


def test_file_downloader_increment(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
    tmp_path,
):
    hwm_store = YAMLHWMStore(path=tmp_path_factory.mktemp("hwmstore"))  # noqa: S306
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        source_path=source_path,
        local_path=local_path,
        hwm_type="file_list",
    )

    # load first batch of the files
    with hwm_store:
        with IncrementalStrategy():
            available = downloader.view_files()
            downloaded = downloader.run()

        # without HWM value all the files are shown and uploaded
        assert len(available) == len(downloaded.successful) == len(upload_test_files)
        assert sorted(available) == sorted(upload_test_files)

    remote_file_folder = RemoteFolder(name=source_path, instance=file_all_connections.instance_url)
    file_hwm = FileListHWM(source=remote_file_folder)
    file_hwm_name = file_hwm.qualified_name

    source_files = {RelativePath(file.relative_to(source_path)) for file in upload_test_files}
    assert source_files == hwm_store.get(file_hwm_name).value

    for _ in "first_inc", "second_inc":
        new_file_name = f"{secrets.token_hex(5)}.txt"
        tmp_file = tmp_path / new_file_name
        tmp_file.write_text(f"{secrets.token_hex(10)}")

        file_all_connections.upload_file(tmp_file, source_path / new_file_name)

        with hwm_store:
            with IncrementalStrategy():
                available = downloader.view_files()
                downloaded = downloader.run()

        # without HWM value all the files are shown and uploaded
        assert len(available) == len(downloaded.successful) == 1
        assert downloaded.successful[0].name == tmp_file.name
        assert downloaded.successful[0].read_text() == tmp_file.read_text()
        assert downloaded.skipped_count == 0
        assert downloaded.missing_count == 0
        assert downloaded.failed_count == 0

        source_files.add(RelativePath(new_file_name))
        assert source_files == hwm_store.get(file_hwm_name).value


def test_file_downloader_increment_fail(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
    tmp_path,
):
    hwm_store = YAMLHWMStore(path=tmp_path_factory.mktemp("hwmstore"))
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        source_path=source_path,
        local_path=local_path,
        hwm_type="file_list",
    )

    with hwm_store:
        with IncrementalStrategy():
            available = downloader.view_files()
            downloaded = downloader.run()

            # without HWM value all the files are shown and uploaded
            assert len(available) == len(downloaded.successful) == len(upload_test_files)
            assert sorted(available) == sorted(upload_test_files)

            remote_file_folder = RemoteFolder(name=source_path, instance=file_all_connections.instance_url)
            file_hwm = FileListHWM(source=remote_file_folder)
            file_hwm_name = file_hwm.qualified_name

            # HWM is updated in HWMStore
            source_files = {RelativePath(file.relative_to(source_path)) for file in upload_test_files}
            assert source_files == hwm_store.get(file_hwm_name).value

            for _ in "first_inc", "second_inc":
                new_file_name = f"{secrets.token_hex(5)}.txt"
                tmp_file = tmp_path / new_file_name
                tmp_file.write_text(f"{secrets.token_hex(10)}")

                file_all_connections.upload_file(tmp_file, source_path / new_file_name)

                # while loading data, a crash occurs before exiting the context manager
                with contextlib.suppress(RuntimeError):
                    available = downloader.view_files()
                    downloaded = downloader.run()
                    # simulating a failure after download
                    raise RuntimeError("some exception")

                assert len(available) == len(downloaded.successful) == 1
                assert downloaded.successful[0].name == tmp_file.name
                assert downloaded.successful[0].read_text() == tmp_file.read_text()
                assert downloaded.skipped_count == 0
                assert downloaded.missing_count == 0
                assert downloaded.failed_count == 0

                # HWM is saved after downloading each file, not after exiting from .run
                source_files.add(RelativePath(new_file_name))
                assert source_files == hwm_store.get(file_hwm_name).value


def test_file_downloader_increment_hwm_is_ignored_for_user_input(
    file_all_connections,
    source_path,
    upload_test_files,
    tmp_path_factory,
    tmp_path,
):
    hwm_store = YAMLHWMStore(path=tmp_path_factory.mktemp("hwm_store"))
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_all_connections,
        source_path=source_path,
        local_path=local_path,
        hwm_type="file_list",
        options=FileDownloader.Options(mode="overwrite"),
    )

    with hwm_store:
        with IncrementalStrategy():
            # load first batch of the files
            downloader.run()

            # download files from list
            download_result = downloader.run(upload_test_files)

    # all the files are downloaded, HWM is ignored
    assert len(download_result.successful) == len(upload_test_files)
