import re
import textwrap
from unittest.mock import Mock

import pytest
from etl_entities.hwm import (
    HWM,
    ColumnDateHWM,
    ColumnDateTimeHWM,
    ColumnHWM,
    ColumnIntHWM,
    FileListHWM,
    FileModifiedTimeHWM,
)
from etl_entities.instance import AbsolutePath
from etl_entities.old_hwm import FileListHWM as OldFileListHWM

from onetl.base import BaseFileConnection
from onetl.core import FileFilter, FileLimit
from onetl.file import FileDownloader
from onetl.file.filter import Glob
from onetl.file.limit import MaxFilesCount
from onetl.impl.file_exist_behavior import FileExistBehavior

SUPPORTED_HWM_TYPES = [FileListHWM, FileModifiedTimeHWM]


def test_file_downloader_deprecated_import():
    msg = textwrap.dedent(
        """
        This import is deprecated since v0.8.0:

            from onetl.core import FileDownloader

        Please use instead:

            from onetl.file import FileDownloader
        """,
    )
    with pytest.warns(UserWarning, match=re.escape(msg)):
        from onetl.core import FileDownloader as OldFileDownloader

        assert OldFileDownloader is FileDownloader


@pytest.mark.parametrize(
    "hwm_type",
    [
        "file_list",
        OldFileListHWM,
    ],
)
def test_file_downloader_hwm_type_deprecated(hwm_type):
    warning_msg = 'Passing "hwm_type" to FileDownloader class is deprecated since version 0.10.0'
    connection = Mock(spec=BaseFileConnection)
    connection.instance_url = "abc"

    with pytest.warns(UserWarning, match=warning_msg):
        downloader = FileDownloader(
            connection=connection,
            local_path="/local/path",
            source_path="/source/path",
            hwm_type=hwm_type,
        )

    assert isinstance(downloader.hwm, FileListHWM)
    assert downloader.hwm.entity == AbsolutePath("/source/path")


def test_file_downloader_unknown_hwm_type():
    # fails on pydantic issubclass(hwm_type, OldFileListHWM) in FileDownloader
    with pytest.raises(ValueError):
        FileDownloader(
            connection=Mock(spec=BaseFileConnection),
            local_path="/local/path",
            source_path="/source/path",
            hwm_type="abc",
        )


@pytest.mark.parametrize(
    "hwm_type",
    [
        ColumnIntHWM,
        ColumnDateHWM,
        ColumnDateTimeHWM,
        ColumnHWM,
        HWM,
    ],
)
def test_file_downloader_wrong_hwm_type(hwm_type):
    # pydantic validation fails, as new hwm classes are passed into hwm_type
    with pytest.raises(ValueError):
        FileDownloader(
            connection=Mock(spec=BaseFileConnection),
            local_path="/local/path",
            source_path="/source/path",
            hwm_type=hwm_type,
        )


@pytest.mark.parametrize(
    "hwm_type",
    [
        "file_list",
        OldFileListHWM,
    ],
)
def test_file_downloader_hwm_type_without_source_path(hwm_type):
    warning_msg = "If `hwm` is passed, `source_path` must be specified"
    with pytest.raises(ValueError, match=warning_msg):
        FileDownloader(
            connection=Mock(spec=BaseFileConnection),
            local_path="/local/path",
            hwm_type=hwm_type,
        )


@pytest.mark.parametrize("hwm_type", SUPPORTED_HWM_TYPES)
def test_file_downloader_hwm_without_source_path(hwm_type):
    warning_msg = "If `hwm` is passed, `source_path` must be specified"
    with pytest.raises(ValueError, match=warning_msg):
        FileDownloader(
            connection=Mock(spec=BaseFileConnection),
            local_path="/local/path",
            hwm=hwm_type(name="abc"),
        )


@pytest.mark.parametrize("hwm_type", SUPPORTED_HWM_TYPES)
def test_file_downloader_hwm_autofill_directory(hwm_type):
    downloader = FileDownloader(
        connection=Mock(spec=BaseFileConnection),
        local_path="/local/path",
        source_path="/source/path",
        hwm=hwm_type(name="abc"),
    )
    assert downloader.hwm.entity == AbsolutePath("/source/path")


@pytest.mark.parametrize("hwm_type", SUPPORTED_HWM_TYPES)
def test_file_downloader_hwm_with_same_directory(hwm_type):
    downloader = FileDownloader(
        connection=Mock(spec=BaseFileConnection),
        local_path="/local/path",
        source_path="/source/path",
        hwm=hwm_type(name="abc", directory="/source/path"),
    )
    assert downloader.hwm.entity == AbsolutePath("/source/path")


@pytest.mark.parametrize("hwm_type", SUPPORTED_HWM_TYPES)
def test_file_downloader_hwm_with_different_directory_error(hwm_type):
    error_msg = "Passed `hwm.directory` is different from `source_path`"
    with pytest.raises(ValueError, match=error_msg):
        FileDownloader(
            connection=Mock(spec=BaseFileConnection),
            local_path="/local/path",
            source_path="/source/path",
            hwm=hwm_type(name="abc", directory="/another/path"),
        )


def test_file_downloader_filter_default():
    downloader = FileDownloader(
        connection=Mock(spec=BaseFileConnection),
        local_path="/local/path",
    )

    assert downloader.filters == []


def test_file_downloader_filter_none():
    with pytest.warns(UserWarning, match=re.escape("filter=None is deprecated in v0.8.0, use filters=[] instead")):
        downloader = FileDownloader(
            connection=Mock(spec=BaseFileConnection),
            local_path="/local/path",
            filter=None,
        )

    assert downloader.filters == []


@pytest.mark.parametrize(
    "file_filter",
    [
        FileFilter(glob="*.txt"),
        Glob("*.txt"),
    ],
)
def test_file_downloader_filter_legacy(file_filter):
    with pytest.warns(UserWarning, match=re.escape("filter=... is deprecated in v0.8.0, use filters=[...] instead")):
        downloader = FileDownloader(
            connection=Mock(spec=BaseFileConnection),
            local_path="/local/path",
            filter=file_filter,
        )

    assert downloader.filters == [file_filter]


def test_file_downloader_limit_default():
    downloader = FileDownloader(
        connection=Mock(spec=BaseFileConnection),
        local_path="/local/path",
    )

    assert downloader.limits == []


def test_file_downloader_limit_none():
    with pytest.warns(UserWarning, match=re.escape("limit=None is deprecated in v0.8.0, use limits=[] instead")):
        downloader = FileDownloader(
            connection=Mock(spec=BaseFileConnection),
            local_path="/local/path",
            limit=None,
        )

    assert downloader.limits == []


@pytest.mark.parametrize(
    "file_limit",
    [
        FileLimit(count_limit=100),
        MaxFilesCount(100),
    ],
)
def test_file_downloader_limit_legacy(file_limit):
    with pytest.warns(UserWarning, match=re.escape("limit=... is deprecated in v0.8.0, use limits=[...] instead")):
        downloader = FileDownloader(
            connection=Mock(spec=BaseFileConnection),
            local_path="/local/path",
            limit=file_limit,
        )

    assert downloader.limits == [file_limit]


@pytest.mark.parametrize(
    "options, value",
    [
        ({}, FileExistBehavior.ERROR),
        ({"if_exists": "error"}, FileExistBehavior.ERROR),
        ({"if_exists": "ignore"}, FileExistBehavior.IGNORE),
        ({"if_exists": "replace_file"}, FileExistBehavior.REPLACE_FILE),
        ({"if_exists": "replace_entire_directory"}, FileExistBehavior.REPLACE_ENTIRE_DIRECTORY),
    ],
)
def test_file_downloader_options_if_exists(options, value):
    assert FileDownloader.Options(**options).if_exists == value


@pytest.mark.parametrize(
    "options, value, message",
    [
        (
            {"mode": "replace_file"},
            FileExistBehavior.REPLACE_FILE,
            "Option `FileDownloader.Options(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `FileDownloader.Options(if_exists=...)` instead",
        ),
        (
            {"mode": "replace_entire_directory"},
            FileExistBehavior.REPLACE_ENTIRE_DIRECTORY,
            "Option `FileDownloader.Options(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `FileDownloader.Options(if_exists=...)` instead",
        ),
        (
            {"mode": "overwrite"},
            FileExistBehavior.REPLACE_FILE,
            "Mode `overwrite` is deprecated since v0.9.0 and will be removed in v1.0.0. " "Use `replace_file` instead",
        ),
        (
            {"mode": "delete_all"},
            FileExistBehavior.REPLACE_ENTIRE_DIRECTORY,
            "Mode `delete_all` is deprecated since v0.9.0 and will be removed in v1.0.0. "
            "Use `replace_entire_directory` instead",
        ),
    ],
)
def test_file_downloader_options_mode_deprecated(options, value, message):
    with pytest.warns(UserWarning, match=re.escape(message)):
        options = FileDownloader.Options(**options)
        assert options.if_exists == value


def test_file_downloader_options_modes_wrong():
    with pytest.raises(ValueError, match="value is not a valid enumeration member"):
        FileDownloader.Options(mode="wrong_mode")
