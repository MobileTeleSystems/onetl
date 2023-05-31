import logging
import re

import pytest

from onetl.file.filter import ExcludeDir, Glob, match_all_filters
from onetl.impl import RemoteDirectory, RemoteFile, RemotePathStat


@pytest.mark.parametrize(
    "failed_filters, path",
    [
        (None, RemoteFile(path="file1.csv", stats=RemotePathStat(st_size=10 * 1024, st_mtime=50))),
        (None, RemoteFile(path="exclude1/file3.csv", stats=RemotePathStat(st_size=20 * 1024, st_mtime=50))),
        (
            [Glob("*.csv")],
            RemoteFile(path="exclude2/nested/file4.txt", stats=RemotePathStat(st_size=30 * 1024, st_mtime=50)),
        ),
        (
            [Glob("*.csv")],
            RemoteFile(path="/exclude1/absolute/path.txt", stats=RemotePathStat(st_size=40 * 1024, st_mtime=50)),
        ),
        (
            [Glob("*.csv"), ExcludeDir("/exclude2")],
            RemoteFile(path="/exclude2/absolute/path.txt", stats=RemotePathStat(st_size=50 * 1024, st_mtime=50)),
        ),
        (None, RemoteDirectory("some.txt")),
        (None, RemoteDirectory("exclude1")),
        (None, RemoteDirectory("exclude2/nested")),
        ([ExcludeDir("/exclude2")], RemoteDirectory("/exclude2/absolute")),
    ],
)
def test_match_all_filters(failed_filters, path, caplog):
    filters = [Glob("*.csv"), ExcludeDir("/exclude2")]
    with caplog.at_level(logging.DEBUG):
        success = not failed_filters
        assert match_all_filters(path, filters) == success

        if failed_filters:
            filters_list_str = re.escape(repr(failed_filters))
            message = f"'{path}'.* does NOT MATCH filters {filters_list_str}"
        else:
            message = f"'{path}'.* does match all filters"

        assert re.search(message, caplog.text)

    assert match_all_filters(path, [])
