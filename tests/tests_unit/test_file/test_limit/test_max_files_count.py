from onetl.file.limit import MaxFilesCount
from onetl.impl import RemoteDirectory, RemoteFile, RemotePathStat


def test_max_files_count():
    limit = MaxFilesCount(3)
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
