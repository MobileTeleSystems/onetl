from onetl.file.limit import MaxFilesCount, reset_limits
from onetl.impl import RemoteFile, RemotePathStat


def test_reset_limits():
    limit1 = MaxFilesCount(3)
    limit2 = MaxFilesCount(10)
    limits = [limit1, limit2]

    file = RemoteFile(path="file1.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))

    for _ in range(3):
        limit1.stops_at(file)
        limit2.stops_at(file)

    assert limit1.is_reached
    assert not limit2.is_reached

    new_limits = reset_limits(limits)
    # new limits list is different from original, but may contain the same elements
    assert new_limits is not limits

    for limit in new_limits:
        assert not limit.is_reached
