import logging

import pytest

from onetl.connection import SparkLocalFS

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection]


def test_spark_local_fs_check(spark, caplog):
    local_fs = SparkLocalFS(spark=spark)

    with caplog.at_level(logging.INFO):
        assert local_fs.check() == local_fs

    assert "|SparkLocalFS|" in caplog.text

    assert "Connection is available." in caplog.text
