import logging

from onetl.file.limit import MaxFilesCount, limits_stop_at
from onetl.impl import RemoteFile, RemotePathStat


def test_limits_stop_at(caplog):
    limit1 = MaxFilesCount(3)
    limit2 = MaxFilesCount(10)
    limits = [limit1, limit2]

    file = RemoteFile(path="file1.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))

    assert not limits_stop_at(file, limits)
    assert not limit1.is_reached
    assert not limit2.is_reached

    assert not limits_stop_at(file, limits)
    assert not limit1.is_reached
    assert not limit2.is_reached

    # limit is reached - all check are True, input does not matter
    with caplog.at_level(logging.DEBUG):
        assert limits_stop_at(file, limits)
        assert limit1.is_reached
        assert not limit2.is_reached

        message = "|FileLimit| Limits [MaxFilesCount(3)] are reached"
        assert message in caplog.text
