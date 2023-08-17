import re
import socket
from unittest.mock import Mock

import pytest

from onetl.connection import SparkLocalFS

pytestmark = [pytest.mark.local_fs, pytest.mark.file_df_connection, pytest.mark.connection]


def test_spark_local_fs_spark_local(spark_mock):
    conn = SparkLocalFS(spark=spark_mock)
    assert conn.spark == spark_mock
    assert conn.instance_url == f"file://{socket.getfqdn()}"


@pytest.mark.parametrize("master", ["k8s", "yarn"])
def test_spark_local_fs_spark_non_local(spark_mock, master):
    spark_mock.conf = Mock()
    spark_mock.conf.get = Mock(return_value=master)

    msg = re.escape("Currently supports only spark.master='local'")
    with pytest.raises(ValueError, match=msg):
        SparkLocalFS(spark=spark_mock)
