import pytest

from onetl.connection import HDFS


class TestHDFS:
    def test_hdfs_wrong_source_check(self):
        hdfs = HDFS(host="hive1", port=1234)

        with pytest.raises(RuntimeError):
            hdfs.check()
