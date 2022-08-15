from unittest.mock import Mock

import pytest
from etl_entities import HWM, ColumnHWM, DateHWM, DateTimeHWM, IntHWM

from onetl.core import FileDownloader


def test_file_downloader_unknown_hwm_type():
    with pytest.raises(KeyError, match="Unknown HWM type 'abc'"):
        FileDownloader(
            connection=Mock(),
            local_path="/path",
            source_path="/path",
            hwm_type="abc",
        )


@pytest.mark.parametrize(
    "hwm_type, hwm_type_name",
    [
        ("byte", "IntHWM"),
        ("integer", "IntHWM"),
        ("short", "IntHWM"),
        ("long", "IntHWM"),
        ("date", "DateHWM"),
        ("timestamp", "DateTimeHWM"),
        (IntHWM, "IntHWM"),
        (DateHWM, "DateHWM"),
        (DateTimeHWM, "DateTimeHWM"),
        (HWM, "HWM"),
        (ColumnHWM, "ColumnHWM"),
    ],
)
def test_file_downloader_wrong_hwm_type(hwm_type, hwm_type_name):
    with pytest.raises(ValueError, match=f"`hwm_type` class should be a inherited from FileHWM, got {hwm_type_name}"):
        FileDownloader(
            connection=Mock(),
            local_path="/path",
            source_path="/path",
            hwm_type=hwm_type,
        )


def test_file_downloader_hwm_type_without_source_path():
    with pytest.raises(ValueError, match="If `hwm_type` is passed, `source_path` must be specified"):
        FileDownloader(
            connection=Mock(),
            local_path="/path",
            hwm_type="file_list",
        )
