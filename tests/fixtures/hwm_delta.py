import secrets
from datetime import date, datetime, timedelta, timezone

import pytest
from etl_entities.hwm import (
    ColumnDateHWM,
    ColumnDateTimeHWM,
    ColumnIntHWM,
    FileListHWM,
    FileModifiedTimeHWM,
)

from onetl.impl.remote_file import RemoteFile
from onetl.impl.remote_path import RemotePath
from onetl.impl.remote_path_stat import RemotePathStat


def file_with_mtime(mtime: datetime) -> RemoteFile:
    return RemoteFile(path=RemotePath(secrets.token_hex(5)), stats=RemotePathStat(st_mtime=mtime.timestamp()))


HWMS_WITH_VALUE = [
    (
        ColumnIntHWM(
            name=secrets.token_hex(5),
            # no source
            expression=secrets.token_hex(5),
            value=10,
        ),
        5,
    ),
    (
        ColumnIntHWM(
            name=secrets.token_hex(5),
            source=secrets.token_hex(5),
            expression=secrets.token_hex(5),
            value=10,
        ),
        5,
    ),
    (
        ColumnDateHWM(
            name=secrets.token_hex(5),
            source=secrets.token_hex(5),
            expression=secrets.token_hex(5),
            value=date(year=2023, month=8, day=15),
        ),
        timedelta(days=31),
    ),
    (
        ColumnDateTimeHWM(
            name=secrets.token_hex(5),
            source=secrets.token_hex(5),
            expression=secrets.token_hex(5),
            value=datetime(year=2023, month=8, day=15, hour=11, minute=22, second=33),
        ),
        timedelta(seconds=50),
    ),
    (
        FileListHWM(
            name=secrets.token_hex(5),
            # not directory
            value=["/some/path", "/another.file"],
        ),
        "/third.file",
    ),
    (
        FileListHWM(
            name=secrets.token_hex(5),
            directory="/absolute/path",
            value=["/absolute/path/file1", "/absolute/path/file2"],
        ),
        "/absolute/path/file3",
    ),
    (
        FileModifiedTimeHWM(
            name=secrets.token_hex(5),
            # no directory
            value=datetime(2025, 1, 1, 11, 22, 33, 456789, tzinfo=timezone.utc),
        ),
        file_with_mtime(datetime(2025, 1, 1, 22, 33, 44, 567890, tzinfo=timezone.utc)),
    ),
    (
        FileModifiedTimeHWM(
            name=secrets.token_hex(5),
            directory="/absolute/path",
            value=datetime(2025, 1, 1, 11, 22, 33, 456789, tzinfo=timezone.utc),
        ),
        file_with_mtime(datetime(2025, 1, 1, 22, 33, 44, 567890, tzinfo=timezone.utc)),
    ),
]


@pytest.fixture(params=HWMS_WITH_VALUE)
def hwm_delta(request):
    return request.param
