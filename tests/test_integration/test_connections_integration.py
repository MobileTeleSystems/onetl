from getpass import getuser
from unittest.mock import patch

import pytest
from hdfs import Client, HdfsError
from hdfs.ext.kerberos import KerberosClient
from paramiko import SFTPClient
from pathlib import Path

from onetl.connection import HDFS, SFTP, FTP, FTPS
from onetl.connection.file_connection import hdfs


class TestFileConnectionIntegration:

    # HDFS

    @patch.object(hdfs, "kinit")
    def test_hdfs_connection_with_keytab(self, kinit, hdfs_server):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port, user=getuser(), password="", keytab="/path/to/keytab")
        assert isinstance(hdfs.client, KerberosClient)

    @patch.object(hdfs, "kinit")
    def test_hdfs_connection_with_password(self, kinit, hdfs_server):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port, user=getuser(), password="pwd")
        assert isinstance(hdfs.client, KerberosClient)

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_hdfs_connection_rm_dir_recursive(self, hdfs_server, hdfs_files, path_type):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port)
        hdfs.rmdir(path_type("/export/news_parse/"), recursive=True)

        assert not hdfs.client.list("/export")

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_hdfs_connection_rmdir_non_empty(self, hdfs_server, hdfs_files, path_type):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port)

        with pytest.raises(HdfsError):
            hdfs.rmdir(path_type("/export/news_parse/"))

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_hdfs_connection_mkdir(self, hdfs_server, hdfs_files, path_type):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port)
        hdfs.mkdir(path_type("/some_dir"))

        assert Path("some_dir") in hdfs.listdir("/")

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_hdfs_connection_rename(self, hdfs_server, hdfs_files, path_type):
        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port)
        hdfs.rename(
            source=path_type("/export/news_parse/exclude_dir/file_5.txt"),
            target=path_type("/export/news_parse/exclude_dir/file_55.txt"),
        )

        list_dir = hdfs.listdir("/export/news_parse/exclude_dir/")

        assert Path("file_55.txt") in list_dir
        assert Path("file_5.txt") not in list_dir

    # SFTP

    def test_sftp_rewrite_cached_property_client(self, sftp_server, hdfs_server):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)
        ssh_client = sftp.client

        hdfs = HDFS(host=hdfs_server.host, port=hdfs_server.port, user=getuser(), password="")
        hdfs_client = hdfs.client

        assert isinstance(ssh_client, SFTPClient)
        assert isinstance(hdfs_client, Client)

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_sftp_connection_rm_dir_recursive(self, sftp_server, sftp_files, path_type):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)
        sftp.rmdir(path_type("/export/news_parse/"), recursive=True)

        assert not sftp.client.listdir("/export")

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_sftp_connection_rmdir_non_empty(self, sftp_server, sftp_files, path_type):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)

        with pytest.raises(OSError):
            sftp.rmdir(path_type("/export/news_parse/"))

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_sftp_connection_mkdir(self, sftp_server, sftp_files, path_type):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)
        sftp.mkdir(path_type("/some_dir"))

        assert Path("some_dir") in sftp.listdir("/")

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_sftp_connection_rename(self, sftp_server, sftp_files, path_type):
        sftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)
        sftp.rename(
            source=path_type("/export/news_parse/exclude_dir/file_5.txt"),
            target=path_type("/export/news_parse/exclude_dir/file_55.txt"),
        )

        list_dir = sftp.listdir("/export/news_parse/exclude_dir")

        assert Path("file_55.txt") in list_dir
        assert Path("file_5.txt") not in list_dir

    # FTP

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_ftp_connection_rm_dir_recursive(self, ftp_server, ftp_files, path_type):
        ftp = FTP(user=ftp_server.user, password=ftp_server.password, host=ftp_server.host, port=ftp_server.port)
        ftp.rmdir(path_type("/export/news_parse/"), recursive=True)

        assert not ftp.client.listdir("/export")

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_ftp_connection_rmdir_non_empty(self, ftp_server, ftp_files, path_type):
        ftp = FTP(user=ftp_server.user, password=ftp_server.password, host=ftp_server.host, port=ftp_server.port)

        with pytest.raises(OSError):
            ftp.rmdir(path_type("/export/news_parse/"))

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_ftp_connection_mkdir(self, ftp_server, ftp_files, path_type):
        ftp = FTP(user=ftp_server.user, password=ftp_server.password, host=ftp_server.host, port=ftp_server.port)
        ftp.mkdir(path_type("/some_dir"))

        assert Path("some_dir") in ftp.listdir("/")

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_ftp_connection_rename(self, ftp_server, ftp_files, path_type):
        ftp = FTP(user=ftp_server.user, password=ftp_server.password, host=ftp_server.host, port=ftp_server.port)
        ftp.rename(
            source=path_type("/export/news_parse/exclude_dir/file_5.txt"),
            target=path_type("/export/news_parse/exclude_dir/file_55.txt"),
        )

        list_dir = ftp.listdir("/export/news_parse/exclude_dir")

        assert Path("file_55.txt") in list_dir
        assert Path("file_5.txt") not in list_dir

    # FTPS

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_ftps_connection_rm_dir_recursive(self, ftps_server, ftps_files, path_type):
        ftps = FTPS(user=ftps_server.user, password=ftps_server.user, host=ftps_server.host, port=ftps_server.port)
        ftps.rmdir(path_type("/export/news_parse/"), recursive=True)

        assert not ftps.client.listdir("/export")

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_ftps_connection_rmdir_non_empty(self, ftps_server, ftps_files, path_type):
        ftps = FTPS(user=ftps_server.user, password=ftps_server.user, host=ftps_server.host, port=ftps_server.port)

        with pytest.raises(OSError):
            ftps.rmdir(path_type("/export/news_parse/"))

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_ftps_connection_mkdir(self, ftps_server, ftps_files, path_type):
        ftps = FTPS(user=ftps_server.user, password=ftps_server.user, host=ftps_server.host, port=ftps_server.port)
        ftps.mkdir(path_type("/some_dir"))

        assert Path("some_dir") in ftps.listdir("/")

    @pytest.mark.parametrize("path_type", [str, Path])
    def test_ftps_connection_rename(self, ftps_server, ftps_files, path_type):
        ftps = FTPS(user=ftps_server.user, password=ftps_server.user, host=ftps_server.host, port=ftps_server.port)
        ftps.rename(
            source=path_type("/export/news_parse/exclude_dir/file_5.txt"),
            target=path_type("/export/news_parse/exclude_dir/file_55.txt"),
        )

        list_dir = ftps.listdir("/export/news_parse/exclude_dir")

        assert Path("file_55.txt") in list_dir
        assert Path("file_5.txt") not in list_dir
