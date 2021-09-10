import ftplib  # noqa: S402
import posixpath
from logging import getLogger
from dataclasses import dataclass

from ftputil import FTPHost, session as ftp_session

from onetl.connection.file_connection.file_connection import FileConnection

log = getLogger(__name__)


@dataclass(frozen=True)
class FTP(FileConnection):
    port: int = 22

    def get_client(self):
        """
        Returns a FTP connection object
        """

        session_factory = ftp_session.session_factory(
            base_class=ftplib.FTP,
            port=self.port,
            encrypt_data_channel=True,
            debug_level=0,
        )

        return FTPHost(
            self.host,
            self.login,
            self.password,
            session_factory=session_factory,
        )

    def is_dir(self, top, item):
        return self.client.path.isdir(posixpath.join(top, self.get_name(item)))

    def get_name(self, item):
        return item

    def download_file(self, remote_file_path, local_file_path):
        self.client.download(remote_file_path, local_file_path)
        log.info(f'Successfully download _file {remote_file_path} remote SFTP to {local_file_path}')

    def remove_file(self, remote_file_path):
        self.client.remove(remote_file_path)
        log.info(f'Successfully removed file {remote_file_path}')

    def _listdir(self, path):
        return self.client.listdir(path)
