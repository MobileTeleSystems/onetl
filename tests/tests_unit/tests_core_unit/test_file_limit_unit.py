import re
import textwrap

import pytest

from onetl.core import FileLimit
from onetl.impl import RemoteDirectory, RemoteFile, RemotePathStat


def test_file_limit():
    warning_msg = textwrap.dedent(
        """
        Using FileLimit is deprecated since v0.8.0 and will be removed in v1.0.0.

        Please replace:
            from onetl.core import FileLimit

            limit=FileLimit(count_limit=3)

        With:
            from onetl.file.limit import MaxFilesCount

            limits=[MaxFilesCount(3)]
        """,
    ).strip()
    with pytest.warns(UserWarning, match=re.escape(warning_msg)):
        file_limit = FileLimit(count_limit=3)

    assert not file_limit.is_reached

    directory = RemoteDirectory("some")
    file1 = RemoteFile(path="file1.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))
    file2 = RemoteFile(path="file2.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))
    file3 = RemoteFile(path="nested/file3.csv", stats=RemotePathStat(st_size=20 * 1024, st_mtime=50))
    file4 = RemoteFile(path="nested/file4.csv", stats=RemotePathStat(st_size=20 * 1024, st_mtime=50))

    assert not file_limit.stops_at(file1)
    assert not file_limit.is_reached

    assert not file_limit.stops_at(file2)
    assert not file_limit.is_reached

    # directories are not checked by limit
    assert not file_limit.stops_at(directory)
    assert not file_limit.is_reached

    # limit is reached - all check are True, input does not matter
    assert file_limit.stops_at(file3)
    assert file_limit.is_reached

    assert file_limit.stops_at(file4)
    assert file_limit.is_reached

    assert file_limit.stops_at(directory)
    assert file_limit.is_reached

    # reset internal state
    file_limit.reset()

    assert not file_limit.stops_at(file1)
    assert not file_limit.is_reached

    # limit does not remember each file, so if duplicates are present, they can affect the result
    assert not file_limit.stops_at(file1)
    assert not file_limit.is_reached

    assert file_limit.stops_at(file1)
    assert file_limit.is_reached
