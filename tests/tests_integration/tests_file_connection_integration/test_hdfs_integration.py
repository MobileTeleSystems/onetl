from getpass import getuser
from unittest.mock import patch

import pytest
from hdfs.ext.kerberos import KerberosClient

from onetl.connection import HDFS
from onetl.connection.file_connection import hdfs


@patch.object(hdfs, "kinit")
def test_hdfs_connection_with_keytab(kinit, hdfs_server):
    hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port, user=getuser(), password="", keytab="/path/to/keytab")
    assert isinstance(hdfs.client, KerberosClient)


@patch.object(hdfs, "kinit")
def test_hdfs_connection_with_password(kinit, hdfs_server):
    hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port, user=getuser(), password="pwd")
    assert isinstance(hdfs.client, KerberosClient)


def test_hdfs_wrong_source_check():
    hdfs = HDFS(host="hive1", port=1234)

    with pytest.raises(RuntimeError):
        hdfs.check()
