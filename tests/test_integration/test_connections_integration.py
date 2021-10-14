from getpass import getuser
from unittest.mock import patch

from paramiko import SFTPClient
from hdfs import Client
from hdfs.ext.kerberos import KerberosClient

from onetl.connection.file_connection import SFTP, HDFS


class TestFileConnectionIntegration:
    @patch("onetl.connection.kereberos_helpers.KerberosMixin.kinit")
    def test_hdfs_connection_with_keytab(self, kinit):
        hdfs = HDFS(host="hive2", port=50070, user=getuser(), password="", extra={"keytab": "/path/to/keytab"})
        assert isinstance(hdfs.client, KerberosClient)

    @patch("onetl.connection.kereberos_helpers.KerberosMixin.kinit")
    def test_hdfs_connection_with_password(self, kinit):
        hdfs = HDFS(host="hive2", port=50070, user=getuser(), password="pwd")
        assert isinstance(hdfs.client, KerberosClient)

    def test_rewrite_cached_property_client(self, sftp_server):

        ssh = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)
        ssh_client = ssh.client

        hdfs = HDFS(host="hive2", port=50070, user=getuser(), password="")
        hdfs_client = hdfs.client

        assert isinstance(ssh_client, SFTPClient)
        assert isinstance(hdfs_client, Client)
