from getpass import getuser
from pathlib import PurePosixPath
from unittest.mock import patch

import pytest
from hdfs import Client
from hdfs.ext.kerberos import KerberosClient
from paramiko import SFTPClient

from onetl.connection import HDFS, SFTP
from onetl.connection.file_connection import hdfs


class TestFileConnectionIntegration:
    @patch.object(hdfs, "kinit")
    def test_hdfs_connection_with_keytab(self, kinit, hdfs_server):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port, user=getuser(), password="", keytab="/path/to/keytab")
        assert isinstance(hdfs.client, KerberosClient)

    @patch.object(hdfs, "kinit")
    def test_hdfs_connection_with_password(self, kinit, hdfs_server):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port, user=getuser(), password="pwd")
        assert isinstance(hdfs.client, KerberosClient)

    @pytest.mark.parametrize("path_type", [str, PurePosixPath])
    def test_rm_dir_recursive(self, file_connection, upload_test_files, path_type):
        file_connection.rmdir(path_type("/export/news_parse/"), recursive=True)

        assert not file_connection.listdir("/export")

    @pytest.mark.parametrize("path_type", [str, PurePosixPath])
    def test_rmdir_non_empty(self, file_connection, upload_test_files, path_type):

        with pytest.raises(Exception):
            file_connection.rmdir(path_type("/export/news_parse/"))

    @pytest.mark.parametrize("path_type", [str, PurePosixPath])
    def test_rmdir_fake_dir(self, file_connection, upload_test_files, path_type):
        # Does not raise Exception

        file_connection.rmdir(path_type("/some/fake/dir"))

    @pytest.mark.parametrize("path_type", [str, PurePosixPath])
    def test_mkdir(self, file_connection, upload_test_files, path_type):
        file_connection.mkdir(path_type("/some_dir"))

        assert PurePosixPath("some_dir") in file_connection.listdir("/")

    @pytest.mark.parametrize("path_type", [str, PurePosixPath])
    def test_rename_file(self, file_connection, upload_test_files, path_type):
        file_connection.rename_file(
            source_file_path=path_type("/export/news_parse/exclude_dir/file_5.txt"),
            target_file_path=path_type("/export/news_parse/exclude_dir/file_55.txt"),
        )

        list_dir = file_connection.listdir("/export/news_parse/exclude_dir/")

        assert PurePosixPath("file_55.txt") in list_dir
        assert PurePosixPath("file_5.txt") not in list_dir

    def test_rewrite_cached_property_client(self, sftp_server, hdfs_server):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)
        ssh_client = sftp.client

        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port, user=getuser(), password="")
        hdfs_client = hdfs.client

        assert isinstance(ssh_client, SFTPClient)
        assert isinstance(hdfs_client, Client)
