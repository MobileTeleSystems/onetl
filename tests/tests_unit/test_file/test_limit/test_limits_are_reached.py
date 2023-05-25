import logging

from onetl.file.limit import MaxFilesCount, limits_reached
from onetl.impl import RemoteFile, RemotePathStat


def test_limits_reached(caplog):
    limit1 = MaxFilesCount(3)
    limit2 = MaxFilesCount(10)
    limits = [limit1, limit2]
    assert not limits_reached(limits)

    file = RemoteFile(path="file1.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))

    assert not limit1.stops_at(file)
    assert not limits_reached(limits)

    assert not limit1.stops_at(file)
    assert not limits_reached(limits)

    # limit is reached - all check are True, input does not matter
    with caplog.at_level(logging.DEBUG):
        assert limit1.stops_at(file)
        assert limits_reached(limits)

        message = "|FileLimit| Limits [MaxFilesCount(3)] are reached"
        assert message in caplog.text
