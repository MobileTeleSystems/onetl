from onetl.core.file_result.file_set import FileSet
from onetl.impl import RemoteFile, RemoteFileStat


def test_file_set():
    files = [
        RemoteFile(path="a/b/c", stats=RemoteFileStat(st_size=10, st_mtime=50)),
        RemoteFile(path="a/b/c", stats=RemoteFileStat(st_size=10, st_mtime=50)),
        RemoteFile(path="a/b/c/d", stats=RemoteFileStat(st_size=20, st_mtime=50)),
    ]

    file_set = FileSet(files)

    assert file_set
    assert len(file_set) == 2
    assert file_set == set(files)

    for file in files:
        assert file in file_set

    assert file_set.total_size == 10 + 20

    file_set.add(RemoteFile(path="a/b/c/e", stats=RemoteFileStat(st_size=30, st_mtime=50)))

    assert len(file_set) == 3
    assert len(file_set.intersection(files)) == 2
    assert len(file_set.difference(files)) == 1

    assert file_set.total_size == 10 + 20 + 30

    file_set.clear()
    assert not file_set

    empty_file_set = FileSet()
    assert not empty_file_set
    assert len(empty_file_set) == 0  # noqa: WPS507
