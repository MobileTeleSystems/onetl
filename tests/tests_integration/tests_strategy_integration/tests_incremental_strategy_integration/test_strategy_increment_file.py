import contextlib
import secrets
import time

import pytest
from etl_entities.hwm import ColumnIntHWM, FileListHWM, FileModifiedTimeHWM
from etl_entities.hwm_store import HWMStoreStackManager
from etl_entities.instance import AbsolutePath

from onetl.file import FileDownloader
from onetl.strategy import IncrementalStrategy
from tests.util.rand import rand_str

SUPPORTED_HWM_TYPES = [FileListHWM, FileModifiedTimeHWM]


@pytest.mark.parametrize("hwm_type", SUPPORTED_HWM_TYPES)
def test_file_downloader_incremental_strategy(
    file_connection_with_path_and_files,
    tmp_path_factory,
    hwm_type,
):
    file_connection, remote_path, uploaded_files = file_connection_with_path_and_files
    if file_connection.__class__.__name__ in {"FTP", "FTPS"} and hwm_type is FileModifiedTimeHWM:
        pytest.skip("Current FTP client truncates st_mtime to minutes, avoid using FileModifiedTimeHWM")

    local_path = tmp_path_factory.mktemp("local_path")
    hwm_store = HWMStoreStackManager.get_current()
    hwm_name = secrets.token_hex(5)

    downloader = FileDownloader(
        connection=file_connection,
        source_path=remote_path,
        local_path=local_path,
        hwm=hwm_type(name=hwm_name),
    )

    # load first batch of the files
    with IncrementalStrategy():
        available = downloader.view_files()
        downloaded = downloader.run()

    # without HWM value all the files are shown and uploaded
    assert len(available) == len(downloaded.successful) == len(uploaded_files)
    assert sorted(available) == sorted(uploaded_files)

    # HWM value is created in HWMStore
    hwm_in_store = hwm_store.get_hwm(hwm_name)
    assert hwm_in_store
    assert hwm_in_store.value
    previous_hwm_value = hwm_in_store.value

    for _ in "first_inc", "second_inc":
        # to change modified time of new files
        time.sleep(1)

        # create new file
        new_file_name = f"{secrets.token_hex(5)}.txt"
        new_file_content = secrets.token_hex(10)
        file_connection.write_text(remote_path / new_file_name, new_file_content)

        with IncrementalStrategy():
            available = downloader.view_files()
            downloaded = downloader.run()

        # without HWM value all the files are shown and uploaded
        assert len(available) == len(downloaded.successful) == 1
        assert downloaded.successful[0].name == new_file_name
        assert downloaded.successful[0].read_text() == new_file_content
        assert not downloaded.skipped
        assert not downloaded.missing
        assert not downloaded.failed

        # HWM value is saved to in HWMStore
        hwm_in_store = hwm_store.get_hwm(hwm_name)
        assert hwm_in_store.value != previous_hwm_value
        previous_hwm_value = hwm_in_store.value


@pytest.mark.parametrize("hwm_type", SUPPORTED_HWM_TYPES)
def test_file_downloader_incremental_strategy_fail(
    file_connection_with_path_and_files,
    tmp_path_factory,
    hwm_type,
):
    file_connection, remote_path, uploaded_files = file_connection_with_path_and_files
    if file_connection.__class__.__name__ in {"FTP", "FTPS"} and hwm_type is FileModifiedTimeHWM:
        pytest.skip("Current FTP client truncates st_mtime to minutes, avoid using FileModifiedTimeHWM")

    local_path = tmp_path_factory.mktemp("local_path")
    hwm_store = HWMStoreStackManager.get_current()
    hwm_name = secrets.token_hex(5)

    downloader = FileDownloader(
        connection=file_connection,
        source_path=remote_path,
        local_path=local_path,
        hwm=hwm_type(name=hwm_name),
    )

    with IncrementalStrategy():  # using the same context manager
        available = downloader.view_files()
        downloaded = downloader.run()

        # without HWM value all the files are shown and uploaded
        assert len(available) == len(downloaded.successful) == len(uploaded_files)
        assert sorted(available) == sorted(uploaded_files)

        # HWM value is created in HWMStore
        hwm_in_store = hwm_store.get_hwm(hwm_name)
        assert hwm_in_store
        assert hwm_in_store.value
        previous_hwm_value = hwm_in_store.value

        for _ in "first_inc", "second_inc":
            # to change modified time of new files
            time.sleep(1)

            # create new file
            new_file_name = f"{secrets.token_hex(5)}.txt"
            new_file_content = secrets.token_hex(10)
            file_connection.write_text(remote_path / new_file_name, new_file_content)

            # while loading data, a crash occurs before exiting the context manager
            with contextlib.suppress(RuntimeError):
                available = downloader.view_files()
                downloaded = downloader.run()
                # simulating a failure after download
                raise RuntimeError("some exception")

            assert len(available) == len(downloaded.successful) == 1
            assert downloaded.successful[0].name == new_file_name
            assert downloaded.successful[0].read_text() == new_file_content
            assert not downloaded.skipped
            assert not downloaded.missing
            assert not downloaded.failed

            # HWM is saved ttoi HWMStore at the end of `FileDownloader.run()` call`,
            # not after exiting from strategy
            hwm_in_store = hwm_store.get_hwm(hwm_name)
            assert hwm_in_store.value != previous_hwm_value
            previous_hwm_value = hwm_in_store.value


@pytest.mark.parametrize("hwm_type", SUPPORTED_HWM_TYPES)
def test_file_downloader_incremental_strategy_hwm_is_ignored_for_user_input(
    file_connection_with_path_and_files,
    tmp_path_factory,
    hwm_type,
):
    file_connection, remote_path, uploaded_files = file_connection_with_path_and_files
    local_path = tmp_path_factory.mktemp("local_path")
    file_hwm_name = secrets.token_hex(5)

    downloader = FileDownloader(
        connection=file_connection,
        source_path=remote_path,
        local_path=local_path,
        hwm=hwm_type(name=file_hwm_name),
        options=FileDownloader.Options(if_exists="replace_file"),
    )

    with IncrementalStrategy():
        # load first batch of the files
        downloader.run()

        # download files from list
        download_result = downloader.run(uploaded_files)

    # all the files are downloaded, HWM is ignored
    assert len(download_result.successful) == len(uploaded_files)


@pytest.mark.parametrize("hwm_type", SUPPORTED_HWM_TYPES)
def test_file_downloader_incremental_strategy_different_hwm_type_in_store(
    file_connection_with_path_and_files,
    tmp_path_factory,
    hwm_type,
):
    hwm_store = HWMStoreStackManager.get_current()
    hwm_name = secrets.token_hex(5)

    file_connection, remote_path, _ = file_connection_with_path_and_files
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_connection,
        source_path=remote_path,
        local_path=local_path,
        hwm=hwm_type(name=hwm_name),
    )

    # HWM Store contains HWM with same name, but different type
    hwm_store.set_hwm(ColumnIntHWM(name=hwm_name, expression="hwm_int"))

    with pytest.raises(TypeError, match="Cannot cast HWM of type .* as .*"):
        with IncrementalStrategy():
            downloader.run()


@pytest.mark.parametrize("hwm_type", SUPPORTED_HWM_TYPES)
def test_file_downloader_incremental_strategy_different_hwm_directory_in_store(
    file_connection_with_path_and_files,
    tmp_path_factory,
    hwm_type,
):
    hwm_store = HWMStoreStackManager.get_current()
    hwm_name = secrets.token_hex(5)

    file_connection, remote_path, _ = file_connection_with_path_and_files
    local_path = tmp_path_factory.mktemp("local_path")

    downloader = FileDownloader(
        connection=file_connection,
        source_path=remote_path,
        local_path=local_path,
        hwm=hwm_type(name=hwm_name),
    )

    # HWM Store contains HWM with same name, but different directory
    hwm_store.set_hwm(hwm_type(name=hwm_name, directory=local_path))
    with pytest.raises(ValueError, match="Detected HWM with different `entity` attribute"):
        with IncrementalStrategy():
            downloader.run()


@pytest.mark.parametrize("hwm_type", SUPPORTED_HWM_TYPES)
@pytest.mark.parametrize("attribute", ["expression", "description"])
def test_file_downloader_incremental_strategy_different_hwm_optional_attribute_in_store(
    file_connection_with_path_and_files,
    tmp_path_factory,
    attribute,
    hwm_type,
):
    hwm_store = HWMStoreStackManager.get_current()
    hwm_name = secrets.token_hex(5)

    file_connection, remote_path, _ = file_connection_with_path_and_files
    local_path = tmp_path_factory.mktemp("local_path")

    old_hwm = hwm_type(name=hwm_name, directory=AbsolutePath(remote_path), expression="some", description="another")
    # HWM Store contains HWM with same name, but different optional attribute
    fake_hwm = old_hwm.copy(update={attribute: rand_str()})
    hwm_store.set_hwm(fake_hwm)

    downloader = FileDownloader(
        connection=file_connection,
        source_path=remote_path,
        local_path=local_path,
        hwm=old_hwm,
    )
    with pytest.warns(UserWarning, match=f"Detected HWM with different `{attribute}` attribute"):
        with IncrementalStrategy():
            downloader.run()

    # attributes from FileDownloader have higher priority, except value
    new_hwm = hwm_store.get_hwm(name=hwm_name)
    assert new_hwm.dict(exclude={"value", "modified_time"}) == old_hwm.dict(exclude={"value", "modified_time"})


@pytest.mark.parametrize("hwm_type", SUPPORTED_HWM_TYPES)
def test_file_downloader_incremental_strategy_hwm_set_twice(
    file_connection_with_path_and_files,
    tmp_path_factory,
    hwm_type,
):
    file_connection, remote_path, _ = file_connection_with_path_and_files
    local_path = tmp_path_factory.mktemp("local_path")

    downloader1 = FileDownloader(
        connection=file_connection,
        source_path=remote_path,
        local_path=local_path,
        hwm=hwm_type(name=secrets.token_hex(5)),
    )

    downloader2 = FileDownloader(
        connection=file_connection,
        source_path=remote_path,
        local_path=local_path,
        hwm=hwm_type(name=secrets.token_hex(5)),
    )

    file_connection.create_dir(remote_path / "different")
    file_connection.write_text(remote_path / "different/file.txt", "abc")
    downloader3 = FileDownloader(
        connection=file_connection,
        source_path=remote_path / "different",
        local_path=local_path,
        hwm=hwm_type(name=secrets.token_hex(5)),
    )

    with IncrementalStrategy():
        downloader1.run()

        with pytest.raises(
            ValueError,
            match="Detected wrong IncrementalStrategy usage.",
        ):
            downloader2.run()

        with pytest.raises(
            ValueError,
            match="Detected wrong IncrementalStrategy usage.",
        ):
            downloader3.run()
