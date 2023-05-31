import pytest

from onetl.file.filter import ExcludeDir
from onetl.impl import RemoteDirectory, RemoteFile, RemotePathStat


@pytest.mark.parametrize(
    "matched, path",
    [
        (True, RemoteFile(path="file1.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))),
        (False, RemoteFile(path="exclude1/file3.csv", stats=RemotePathStat(st_size=20 * 1024, st_mtime=50))),
        (True, RemoteFile(path="exclude2/nested/file4.txt", stats=RemotePathStat(st_size=30 * 1024, st_mtime=50))),
        (True, RemoteFile(path="/exclude1/absolute/path.txt", stats=RemotePathStat(st_size=40 * 1024, st_mtime=50))),
        (True, RemoteFile(path="/exclude2/absolute/path.txt", stats=RemotePathStat(st_size=50 * 1024, st_mtime=50))),
        (True, RemoteDirectory("some.txt")),
        (False, RemoteDirectory("exclude1")),
        (True, RemoteDirectory("exclude2/nested")),
        (True, RemoteDirectory("/exclude2/absolute")),
    ],
)
def test_exclude_dir_match_relative(matched, path):
    file_filter = ExcludeDir("exclude1")

    assert file_filter.match(path) == matched


@pytest.mark.parametrize(
    "matched, path",
    [
        (True, RemoteFile(path="file1.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))),
        (True, RemoteFile(path="exclude1/file3.csv", stats=RemotePathStat(st_size=20 * 1024, st_mtime=50))),
        (True, RemoteFile(path="exclude2/nested/file4.txt", stats=RemotePathStat(st_size=30 * 1024, st_mtime=50))),
        (False, RemoteFile(path="/exclude1/absolute/path.txt", stats=RemotePathStat(st_size=40 * 1024, st_mtime=50))),
        (True, RemoteFile(path="/exclude2/absolute/path.txt", stats=RemotePathStat(st_size=50 * 1024, st_mtime=50))),
        (True, RemoteDirectory("some.txt")),
        (True, RemoteDirectory("exclude1")),
        (True, RemoteDirectory("exclude2/nested")),
        (True, RemoteDirectory("/exclude2/absolute")),
    ],
)
def test_exclude_dir_match_absolute(matched, path):
    file_filter = ExcludeDir("/exclude1")

    assert file_filter.match(path) == matched
