from getpass import getuser
from unittest.mock import patch

from paramiko import SFTPClient
import pytest
from hdfs import Client
from hdfs.ext.kerberos import KerberosClient

from onetl.connection.file_connection import SFTP, HDFS


class TestFileConnectionIntegration:
    @patch("onetl.connection.kereberos_helpers.KerberosMixin.kinit")
    def test_hdfs_connection_with_keytab(self, kinit):
        hdfs = HDFS(host="hive2", port=50070, user=getuser(), password="", keytab="/path/to/keytab")
        assert isinstance(hdfs.client, KerberosClient)

    @patch("onetl.connection.kereberos_helpers.KerberosMixin.kinit")
    def test_hdfs_connection_with_password(self, kinit):
        hdfs = HDFS(host="hive2", port=50070, user=getuser(), password="pwd")
        assert isinstance(hdfs.client, KerberosClient)

    def test_rewrite_cached_property_client(self, sftp_server):

        sftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)
        ssh_client = sftp.client

        hdfs = HDFS(host="hive2", port=50070, user=getuser(), password="")
        hdfs_client = hdfs.client

        assert isinstance(ssh_client, SFTPClient)
        assert isinstance(hdfs_client, Client)

    def test_connection_rm_dir_recursive(self, sftp_server, sftp_files):

        sftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)
        sftp.rmdir(path="/export/news_parse/", recursive=True)

        assert not sftp.client.listdir("/export")

    def test_connection_rmdir_non_empty(self, sftp_server, sftp_files):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)
        with pytest.raises(OSError):
            sftp.rmdir(path="/export/news_parse/")
