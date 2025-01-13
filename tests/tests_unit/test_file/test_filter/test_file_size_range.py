import pytest

from onetl.file.filter import FileSizeRange
from onetl.impl import RemoteDirectory, RemoteFile, RemotePathStat


def test_file_size_range_invalid():
    with pytest.raises(ValueError, match="Either min or max must be specified"):
        FileSizeRange()

    with pytest.raises(ValueError, match="size cannot be negative"):
        FileSizeRange(min=-1)

    with pytest.raises(ValueError, match="size cannot be negative"):
        FileSizeRange(max=-1)

    with pytest.raises(ValueError, match="Min size cannot be greater than max size"):
        FileSizeRange(min="10KB", max="1KB")

    with pytest.raises(ValueError, match="could not parse value and unit from byte string"):
        FileSizeRange(min="wtf")
    with pytest.raises(ValueError, match="could not parse value and unit from byte string"):
        FileSizeRange(max="wtf")


def test_file_size_range_repr():
    assert repr(FileSizeRange(min="10KiB", max="10GiB")) == "FileSizeRange(min='10.0KiB', max='10.0GiB')"


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
def test_file_size_range_parse_units(input: str, expected: int):
    assert FileSizeRange(min=input.replace("B", "b")).min == expected
    assert FileSizeRange(min=input).min == expected
    assert FileSizeRange(max=input.replace("B", "b")).max == expected
    assert FileSizeRange(max=input).max == expected


@pytest.mark.parametrize(
    "matched, path",
    [
        (False, RemoteFile(path="file.csv", stats=RemotePathStat(st_size=1024, st_mtime=50))),
        (True, RemoteFile(path="file.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))),
        (True, RemoteFile(path="file.csv", stats=RemotePathStat(st_size=15 * 1024, st_mtime=50))),
        (True, RemoteFile(path="file.csv", stats=RemotePathStat(st_size=20 * 1024, st_mtime=50))),
        (False, RemoteFile(path="file.csv", stats=RemotePathStat(st_size=30 * 1024, st_mtime=50))),
        (True, RemoteDirectory("some")),
    ],
)
def test_file_size_range_match(matched, path):
    file_filter = FileSizeRange(min="10Kib", max="20Kib")

    assert file_filter.match(path) == matched
