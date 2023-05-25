import re

import pytest

from onetl.file.filter import Regexp
from onetl.impl import RemoteDirectory, RemoteFile, RemotePathStat


def test_regep_invalid():
    with pytest.raises(ValueError, match=r"Invalid regexp: '\?d'"):
        Regexp("?d")


@pytest.mark.parametrize(
    "matched, path",
    [
        (False, RemoteFile(path="file.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))),
        (True, RemoteFile(path="file1.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))),
        (True, RemoteFile(path="nested/file34.csv", stats=RemotePathStat(st_size=20 * 1024, st_mtime=50))),
        (False, RemoteFile(path="/absolute/file567.txt", stats=RemotePathStat(st_size=30 * 1024, st_mtime=50))),
        (True, RemoteFile(path="UPPERCASE123.CSV", stats=RemotePathStat(st_size=30 * 1024, st_mtime=50))),
        (False, RemoteFile(path="no_ext", stats=RemotePathStat(st_size=40 * 1024, st_mtime=50))),
        (True, RemoteDirectory("some")),
        (True, RemoteDirectory("some.csv")),
        (True, RemoteDirectory("some.txt")),
        (True, RemoteDirectory("/absolute/some")),
        (True, RemoteDirectory("/absolute/some.csv")),
        (True, RemoteDirectory("/absolute/some.txt")),
    ],
)
def test_regexp_match_str(matched, path):
    file_filter = Regexp(r"e\d+\.csv")

    assert file_filter.match(path) == matched


@pytest.mark.parametrize(
    "matched, path",
    [
        (False, RemoteFile(path="file.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))),
        (True, RemoteFile(path="file1.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))),
        (True, RemoteFile(path="nested/file34.csv", stats=RemotePathStat(st_size=20 * 1024, st_mtime=50))),
        (False, RemoteFile(path="/absolute/file567.txt", stats=RemotePathStat(st_size=30 * 1024, st_mtime=50))),
        (False, RemoteFile(path="UPPERCASE123.CSV", stats=RemotePathStat(st_size=30 * 1024, st_mtime=50))),
        (False, RemoteFile(path="no_ext", stats=RemotePathStat(st_size=40 * 1024, st_mtime=50))),
        (True, RemoteDirectory("some")),
        (True, RemoteDirectory("some.csv")),
        (True, RemoteDirectory("some.txt")),
        (True, RemoteDirectory("/absolute/some")),
        (True, RemoteDirectory("/absolute/some.csv")),
        (True, RemoteDirectory("/absolute/some.txt")),
    ],
)
def test_regexp_match_pattern(matched, path):
    file_filter = Regexp(re.compile(r"e\d+\.csv"))

    assert file_filter.match(path) == matched
