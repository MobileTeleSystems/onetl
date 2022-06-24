import re

import pytest

from onetl.core import FileFilter
from onetl.impl import RemoteDirectory, RemoteFile, RemoteFileStat


def test_file_filter_no_args():
    with pytest.raises(ValueError):
        FileFilter()


def test_file_filter_both_glob_and_regexp():
    with pytest.raises(ValueError):
        FileFilter(glob="*.csv", regexp=r"\d+\.csv")


@pytest.mark.parametrize(
    "matched, path",
    [
        (True, RemoteFile(path="file1.csv", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50))),
        (True, RemoteFile(path="nested/file3.csv", stats=RemoteFileStat(st_size=20 * 1024, st_mtime=50))),
        (False, RemoteFile(path="/absolute/file4.txt", stats=RemoteFileStat(st_size=30 * 1024, st_mtime=50))),
        (False, RemoteFile(path="no_ext", stats=RemoteFileStat(st_size=40 * 1024, st_mtime=50))),
        (True, RemoteDirectory("some")),
        (True, RemoteDirectory("some.csv")),
        (True, RemoteDirectory("some.txt")),
        (True, RemoteDirectory("/absolute/some")),
        (True, RemoteDirectory("/absolute/some.csv")),
        (True, RemoteDirectory("/absolute/some.txt")),
    ],
)
def test_file_filter_glob(matched, path):
    file_filter = FileFilter(glob="*.csv")

    assert file_filter.match(path) == matched


@pytest.mark.parametrize(
    "matched, path",
    [
        (True, RemoteFile(path="file1.csv", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50))),
        (False, RemoteFile(path="exclude1/file3.csv", stats=RemoteFileStat(st_size=20 * 1024, st_mtime=50))),
        (False, RemoteFile(path="exclude2/nested/file4.txt", stats=RemoteFileStat(st_size=30 * 1024, st_mtime=50))),
        (True, RemoteFile(path="/exclude1/absolute/path.txt", stats=RemoteFileStat(st_size=40 * 1024, st_mtime=50))),
        (True, RemoteFile(path="/exclude2/absolute/path.txt", stats=RemoteFileStat(st_size=50 * 1024, st_mtime=50))),
        (True, RemoteDirectory("some.txt")),
        (False, RemoteDirectory("exclude1")),
        (False, RemoteDirectory("exclude2/nested")),
        (True, RemoteDirectory("/exclude2/absolute")),
    ],
)
def test_file_filter_exclude_dirs_relative(matched, path):
    file_filter = FileFilter(exclude_dirs=["exclude1", "exclude2"])

    assert file_filter.match(path) == matched


@pytest.mark.parametrize(
    "matched, path",
    [
        (True, RemoteFile(path="file1.csv", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50))),
        (True, RemoteFile(path="exclude1/file3.csv", stats=RemoteFileStat(st_size=20 * 1024, st_mtime=50))),
        (True, RemoteFile(path="exclude2/nested/file4.txt", stats=RemoteFileStat(st_size=30 * 1024, st_mtime=50))),
        (False, RemoteFile(path="/exclude1/absolute/path.txt", stats=RemoteFileStat(st_size=40 * 1024, st_mtime=50))),
        (False, RemoteFile(path="/exclude2/absolute/path.txt", stats=RemoteFileStat(st_size=50 * 1024, st_mtime=50))),
        (True, RemoteDirectory("some.txt")),
        (True, RemoteDirectory("exclude1")),
        (True, RemoteDirectory("exclude2/nested")),
        (False, RemoteDirectory("/exclude2/absolute")),
    ],
)
def test_file_filter_exclude_dirs_absolute(matched, path):
    file_filter = FileFilter(exclude_dirs=["/exclude1", "/exclude2"])

    assert file_filter.match(path) == matched


@pytest.mark.parametrize(
    "matched, path",
    [
        (False, RemoteFile(path="file.csv", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50))),
        (True, RemoteFile(path="file1.csv", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50))),
        (True, RemoteFile(path="nested/file34.csv", stats=RemoteFileStat(st_size=20 * 1024, st_mtime=50))),
        (False, RemoteFile(path="/absolute/file567.txt", stats=RemoteFileStat(st_size=30 * 1024, st_mtime=50))),
        (True, RemoteFile(path="UPPERCASE123.CSV", stats=RemoteFileStat(st_size=30 * 1024, st_mtime=50))),
        (False, RemoteFile(path="no_ext", stats=RemoteFileStat(st_size=40 * 1024, st_mtime=50))),
        (True, RemoteDirectory("some")),
        (True, RemoteDirectory("some.csv")),
        (True, RemoteDirectory("some.txt")),
        (True, RemoteDirectory("/absolute/some")),
        (True, RemoteDirectory("/absolute/some.csv")),
        (True, RemoteDirectory("/absolute/some.txt")),
    ],
)
def test_file_filter_regexp_str(matched, path):
    file_filter = FileFilter(regexp=r"e\d+\.csv")

    assert file_filter.match(path) == matched


@pytest.mark.parametrize(
    "matched, path",
    [
        (False, RemoteFile(path="file.csv", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50))),
        (True, RemoteFile(path="file1.csv", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50))),
        (True, RemoteFile(path="nested/file34.csv", stats=RemoteFileStat(st_size=20 * 1024, st_mtime=50))),
        (False, RemoteFile(path="/absolute/file567.txt", stats=RemoteFileStat(st_size=30 * 1024, st_mtime=50))),
        (False, RemoteFile(path="UPPERCASE123.CSV", stats=RemoteFileStat(st_size=30 * 1024, st_mtime=50))),
        (False, RemoteFile(path="no_ext", stats=RemoteFileStat(st_size=40 * 1024, st_mtime=50))),
        (True, RemoteDirectory("some")),
        (True, RemoteDirectory("some.csv")),
        (True, RemoteDirectory("some.txt")),
        (True, RemoteDirectory("/absolute/some")),
        (True, RemoteDirectory("/absolute/some.csv")),
        (True, RemoteDirectory("/absolute/some.txt")),
    ],
)
def test_file_filter_regexp_pattern(matched, path):
    file_filter = FileFilter(regexp=re.compile(r"e\d+\.csv"))

    assert file_filter.match(path) == matched
