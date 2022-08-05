import re
import textwrap

import pytest

from onetl.core import FileSet
from onetl.exception import EmptyFilesError, ZeroFileSizeError
from onetl.impl import LocalPath, RemoteDirectory, RemoteFile, RemoteFileStat


def test_file_set():
    file1 = RemoteFile(path="a/b/c", stats=RemoteFileStat(st_size=10, st_mtime=50))
    file2 = RemoteFile(path="a/b/c/d", stats=RemoteFileStat(st_size=20, st_mtime=50))
    file3 = RemoteFile(path="a/b/c/e", stats=RemoteFileStat(st_size=30, st_mtime=50))

    files = [file1, file1, file2]

    # FileSet is like a ordinary set
    file_set = FileSet(files)

    assert file_set
    assert len(file_set) == 2
    assert file_set == set(files)

    for file in files:
        assert file in file_set

    assert file_set.total_size == 10 + 20

    file_set.add(file3)

    assert len(file_set) == 3
    assert len(file_set.intersection(files)) == 2
    assert len(file_set.difference(files)) == 1

    assert file_set.total_size == 10 + 20 + 30

    # But at the same time it is like list, keeping only unique values
    assert file_set == [file1, file2, file3]
    assert file_set != [file1, file3, file2]
    assert file_set[0] == file1
    assert file_set[1] == file2
    assert file_set.index(file1) == 0
    assert file_set.index(file2) == 1

    file_set.append(file3)
    assert file_set == [file1, file2, file3]

    file_set.clear()
    assert not file_set

    empty_file_set = FileSet()
    assert not empty_file_set
    assert len(empty_file_set) == 0  # noqa: WPS507


def test_file_set_details():
    path1 = RemoteFile(path="a/b/c", stats=RemoteFileStat(st_size=10, st_mtime=50))
    path2 = RemoteDirectory("a/b/c/d")
    path3 = LocalPath("a/b/c/e")
    path4 = "a/b/c/f"

    items = [path1, path2, path3, path4]

    file_set = FileSet(items)

    summary = "4 files (10 Bytes)"
    details = """
        4 files (10 Bytes):
            'a/b/c' (10 Bytes)
            'a/b/c/d' (directory)
            'a/b/c/e' (missing)
            'a/b/c/f'
    """

    assert file_set.details == str(file_set) == textwrap.dedent(details).strip()
    assert file_set.summary == summary

    empty_file_set = FileSet()
    assert empty_file_set.details == empty_file_set.summary == str(empty_file_set) == "No files"


def test_file_set_raise_if_empty():
    empty_file_set = FileSet()

    with pytest.raises(EmptyFilesError, match="There are no files in the set"):
        empty_file_set.raise_if_empty()

    FileSet([LocalPath("some")]).raise_if_empty()


def test_file_set_raise_if_contains_zero_size():
    files = [
        RemoteFile(path="/empty", stats=RemoteFileStat(st_size=0, st_mtime=50)),
        RemoteFile(path="/successful", stats=RemoteFileStat(st_size=10 * 1024, st_mtime=50)),
        LocalPath("cannot/detect/size1"),  # missing file does not mean zero size
    ]

    details = """
        1 file out of 3 have zero size:
            '/empty'
    """

    error_message = re.escape(textwrap.dedent(details).strip())

    with pytest.raises(ZeroFileSizeError, match=error_message):
        FileSet(files).raise_if_contains_zero_size()

    # empty successful files does not mean zero files size
    assert not FileSet().raise_if_contains_zero_size()
