import pytest

from onetl.file.filter import Glob
from onetl.impl import RemoteDirectory, RemoteFile, RemotePathStat


def test_glob_invalid():
    with pytest.raises(ValueError, match="Invalid glob: 'something'"):
        Glob("something")


@pytest.mark.parametrize(
    "matched, path",
    [
        (True, RemoteFile(path="file1.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))),
        (True, RemoteFile(path="nested/file3.csv", stats=RemotePathStat(st_size=20 * 1024, st_mtime=50))),
        (False, RemoteFile(path="/absolute/file4.txt", stats=RemotePathStat(st_size=30 * 1024, st_mtime=50))),
        (False, RemoteFile(path="no_ext", stats=RemotePathStat(st_size=40 * 1024, st_mtime=50))),
        (True, RemoteDirectory("some")),
        (True, RemoteDirectory("some.csv")),
        (True, RemoteDirectory("some.txt")),
        (True, RemoteDirectory("/absolute/some")),
        (True, RemoteDirectory("/absolute/some.csv")),
        (True, RemoteDirectory("/absolute/some.txt")),
    ],
)
def test_glob_match(matched, path):
    file_filter = Glob("*.csv")

    assert file_filter.match(path) == matched
