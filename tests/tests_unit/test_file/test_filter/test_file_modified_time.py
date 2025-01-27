from datetime import datetime, timedelta, timezone

import pytest

from onetl.file.filter import FileModifiedTime
from onetl.impl import RemoteDirectory, RemoteFile, RemotePathStat


def test_file_modified_time_invalid():
    with pytest.raises(ValueError, match="Either since or until must be specified"):
        FileModifiedTime()

    with pytest.raises(ValueError, match="since cannot be greater than until"):
        FileModifiedTime(since=datetime(2025, 1, 1), until=datetime(2024, 1, 1))

    with pytest.raises(ValueError, match="Invalid isoformat string"):
        FileModifiedTime(since="wtf")
    with pytest.raises(ValueError, match="Invalid isoformat string"):
        FileModifiedTime(until="wtf")


# values always timezone-aware
@pytest.mark.parametrize(
    ["input", "expected"],
    [
        (
            datetime(2025, 1, 1),
            datetime(2025, 1, 1).astimezone(),
        ),
        (
            "2025-01-01",
            datetime(2025, 1, 1).astimezone(),
        ),
        (
            datetime(2025, 1, 1, 11, 22, 33, 456789),
            datetime(2025, 1, 1, 11, 22, 33, 456789).astimezone(),
        ),
        (
            "2025-01-01T11:22:33.456789",
            datetime(2025, 1, 1, 11, 22, 33, 456789).astimezone(),
        ),
        (
            datetime(2025, 1, 1, 11, 22, 33, 456789, tzinfo=timezone.utc),
            datetime(2025, 1, 1, 11, 22, 33, 456789, tzinfo=timezone.utc),
        ),
        (
            "2025-01-01T11:22:33.456789+00:00",
            datetime(2025, 1, 1, 11, 22, 33, 456789, tzinfo=timezone.utc),
        ),
        (
            datetime(2025, 1, 1, 11, 22, 33, 456789, tzinfo=timezone(timedelta(hours=3))),
            datetime(2025, 1, 1, 11, 22, 33, 456789, tzinfo=timezone(timedelta(hours=3))),
        ),
        (
            "2025-01-01T11:22:33.456789+03:00",
            datetime(2025, 1, 1, 11, 22, 33, 456789, tzinfo=timezone(timedelta(hours=3))),
        ),
    ],
)
def test_file_modified_time_parse(input: str, expected: datetime):
    assert FileModifiedTime(since=input).since == expected
    assert FileModifiedTime(until=input).until == expected


def test_file_modified_time_repr():
    value = FileModifiedTime(
        since=datetime(2025, 1, 1, 11, 22, 33, 456789, tzinfo=timezone.utc),
        until=datetime(2025, 1, 1, 22, 33, 44, 567890, tzinfo=timezone(timedelta(hours=3))),
    )

    since_str = "2025-01-01T11:22:33.456789+00:00"
    until_str = "2025-01-01T22:33:44.567890+03:00"
    expected = f"FileModifiedTime(since='{since_str}', until='{until_str}')"
    assert repr(value) == expected


# only POSIX timestamps are compared, so all values are in UTC
@pytest.mark.parametrize(
    "matched, mtime",
    [
        (False, datetime(2025, 1, 1, 11, 22, 33, 456788, tzinfo=timezone.utc)),  # since-1ms
        (True, datetime(2025, 1, 1, 11, 22, 33, 456789, tzinfo=timezone.utc)),
        (True, datetime(2025, 1, 1, 22, 33, 44, 567890, tzinfo=timezone.utc)),
        (False, datetime(2025, 1, 1, 22, 33, 44, 567891, tzinfo=timezone.utc)),  # util+1ms
    ],
)
def test_file_modified_time_match(matched: bool, mtime: datetime):
    file_filter = FileModifiedTime(
        since=datetime(2025, 1, 1, 11, 22, 33, 456789, tzinfo=timezone.utc),
        until=datetime(2025, 1, 1, 22, 33, 44, 567890, tzinfo=timezone.utc),
    )

    file = RemoteFile(path="file.csv", stats=RemotePathStat(st_size=0, st_mtime=mtime.timestamp()))
    assert file_filter.match(file) == matched

    directory = RemoteDirectory("some")
    assert file_filter.match(directory)
