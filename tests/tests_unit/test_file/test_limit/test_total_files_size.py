import pytest

from onetl.file.limit import TotalFilesSize
from onetl.impl import RemoteDirectory, RemoteFile, RemotePathStat


def test_total_files_size_invalid():
    with pytest.raises(ValueError, match="Limit should be positive number"):
        TotalFilesSize(0)

    with pytest.raises(ValueError, match="Limit should be positive number"):
        TotalFilesSize(-1)

    with pytest.raises(ValueError, match="could not parse value and unit from byte string"):
        TotalFilesSize("wtf")


def test_total_files_size_repr():
    assert repr(TotalFilesSize("10KiB")) == 'TotalFilesSize("10.0KiB")'


@pytest.mark.parametrize(
    ["input", "expected"],
    [
        ("10", 10),
        ("10B", 10),
        ("10KB", 10_000),
        ("10KiB", 10 * 1024),
        ("10MB", 10_000_000),
        ("10MiB", 10 * 1024 * 1024),
        ("10GB", 10_000_000_000),
        ("10GiB", 10 * 1024 * 1024 * 1024),
    ],
)
def test_total_files_size_parse_units(input: str, expected: int):
    assert TotalFilesSize(input.replace("B", "b")).limit == expected
    assert TotalFilesSize(input).limit == expected


def test_total_files_size():
    limit = TotalFilesSize("30KiB")
    assert not limit.is_reached

    directory = RemoteDirectory("some")
    file1 = RemoteFile(path="file1.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))
    file2 = RemoteFile(path="file2.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))
    file3 = RemoteFile(path="nested/file3.csv", stats=RemotePathStat(st_size=20 * 1024, st_mtime=50))
    file4 = RemoteFile(path="nested/file4.csv", stats=RemotePathStat(st_size=20 * 1024, st_mtime=50))

    assert not limit.stops_at(file1)
    assert not limit.is_reached

    assert not limit.stops_at(file2)
    assert not limit.is_reached

    # directories are not checked by limit
    assert not limit.stops_at(directory)
    assert not limit.is_reached

    # limit is reached - all check are True, input does not matter
    assert limit.stops_at(file3)
    assert limit.is_reached

    assert limit.stops_at(file4)
    assert limit.is_reached

    assert limit.stops_at(directory)
    assert limit.is_reached

    # reset internal state
    limit.reset()

    assert not limit.stops_at(file1)
    assert not limit.is_reached

    # limit does not remember each file, so if duplicates are present, they can affect the result
    assert not limit.stops_at(file1)
    assert not limit.is_reached

    assert limit.stops_at(file1)
    assert limit.is_reached
