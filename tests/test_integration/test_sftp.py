from stat import S_ISDIR
import posixpath
from logging import getLogger
import os
import tempfile

from onetl.connection.file_connection import SFTP
from onetl.downloader import FileDownloader
from onetl.uploader import FileUploader
from tests.lib.common import hashfile

LOG = getLogger(__name__)


def sftp_walk(sftp, remote_path):
    # Kindof a stripped down  version of os.walk, implemented for
    # sftp.
    path = remote_path
    files = []
    folders = []
    for f in sftp.listdir_attr(remote_path):
        if S_ISDIR(f.st_mode):
            folders.append(f.filename)
        else:
            files.append(f.filename)
    yield path, folders, files
    for folder in folders:
        new_path = posixpath.join(remote_path, folder)
        yield from sftp_walk(sftp, new_path)


class TestDownloader:
    def test_downloader_local_path(self, sftp_files, sftp_server, sftp_source_path, test_file_name, test_file_path):
        ftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)
        with tempfile.TemporaryDirectory() as local_path:

            downloader = FileDownloader(
                connection=ftp,
                source_path=sftp_source_path,
                local_path=local_path,
                source_file_pattern="*.csv",
            )

            files = downloader.run()
            assert files == [posixpath.join(local_path, test_file_name)]
            # compare size of files
            assert os.path.getsize(test_file_path) == os.path.getsize(os.path.join(local_path, test_file_name))
            # compare files
            assert hashfile(test_file_path) == hashfile(os.path.join(local_path, test_file_name))

    def test_downloader_wrong_pattern(self, sftp_files, sftp_server, sftp_source_path):
        ftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)
        with tempfile.TemporaryDirectory() as local_path:

            downloader = FileDownloader(
                connection=ftp,
                source_path=sftp_source_path,
                local_path=local_path,
                source_file_pattern="*.wng",
            )
            files = downloader.run()
            assert not files

    def test_downloader_exclude_dirs(self, sftp_files, sftp_server, sftp_source_path):
        ftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)
        with tempfile.TemporaryDirectory() as local_path:

            downloader = FileDownloader(
                connection=ftp,
                source_path=sftp_source_path,
                local_path=local_path,
                source_exclude_dirs=["/export/news_parse/exclude_dir"],
            )

            files = downloader.run()

            assert os.path.join(local_path, "file_1.txt") in files
            assert os.path.join(local_path, "file_5.txt") not in files

    # TODO: сделать тесты атомарными
    def test_downloader_delete_source(self, sftp_client, sftp_files, sftp_server, sftp_source_path):
        ftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)
        with tempfile.TemporaryDirectory() as local_path:

            downloader = FileDownloader(
                connection=ftp,
                source_path=sftp_source_path,
                local_path=local_path,
                delete_source=True,
            )

            downloaded_files = downloader.run()

            current_sftp_files = set()
            for root, _dirs, files in sftp_walk(sftp_client, sftp_source_path):
                for filename in files:
                    current_sftp_files.add(posixpath.join(root, filename))

            assert downloaded_files
            assert not current_sftp_files

    def test_sftp_uploader(self, sftp_server, test_file_name, test_file_path):
        ftp = SFTP(user=sftp_server.user, password=sftp_server.user, host=sftp_server.host, port=sftp_server.port)

        uploader = FileUploader(connection=ftp, target_path="/tmp/test_upload")
        files = [
            test_file_path,
        ]

        uploaded_files = uploader.run(files)
        assert uploaded_files == [f"/tmp/test_upload/{test_file_name}"]
