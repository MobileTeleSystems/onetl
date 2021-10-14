import ftplib  # noqa: S402
import posixpath
from logging import getLogger
from dataclasses import dataclass
from typing import List

from ftputil import FTPHost, session as ftp_session

from onetl.connection.file_connection.file_connection import FileConnection

log = getLogger(__name__)


@dataclass(frozen=True)
class FTP(FileConnection):
    port: int = 21

    def get_client(self) -> "ftputil.host.FTPHost":
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
            self.user,
            self.password,
            session_factory=session_factory,
        )

    def is_dir(self, top: str, item: str) -> bool:
        return self.client.path.isdir(posixpath.join(top, self.get_name(item)))

    def get_name(self, item: str) -> str:
        return item

    def download_file(self, remote_file_path: str, local_file_path: str) -> None:
        self.client.download(remote_file_path, local_file_path)
        log.info(f"Successfully download file {remote_file_path} from remote SFTP to {local_file_path}")

    def remove_file(self, remote_file_path: str) -> None:
        self.client.remove(remote_file_path)
        log.info(f"Successfully removed file {remote_file_path}")

    def mkdir(self, path: str) -> None:
        self.client.mkdir(path)
        log.info(f"Successfully created directory {path}")

    def path_exists(self, path: str) -> bool:
        return self.client.stat(path=path, _exception_for_missing_path=False)

    def upload_file(self, local_file_path: str, remote_file_path: str, *args, **kwargs) -> None:
        self.client.run(local_file_path, remote_file_path)

    def rename(self, source: str, target: str) -> None:
        self.client.rename(source, target)
        log.info(f"Successfully renamed file {source} to {target}")

    def rmdir(self, path: str, recursive: bool) -> None:
        self.client.rmtree(path)
        log.info(f"Successfully deleted {path}")

    def _listdir(self, path: str) -> List:
        return self.client.listdir(path)
