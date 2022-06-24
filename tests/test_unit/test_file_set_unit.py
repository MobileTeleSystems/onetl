from onetl.core import FileSet
from onetl.impl import RemoteFile, RemoteFileStat


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
