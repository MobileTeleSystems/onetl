from dataclasses import dataclass
from logging import getLogger
from typing import List, Optional

from smbclient import SambaClient

from onetl.connection.file_connection.file_connection import FileConnection

log = getLogger(__name__)


@dataclass(frozen=True)
class Samba(FileConnection):

    port: int = 445
    domain: Optional[str] = None

    def get_client(self) -> SambaClient:

        return SambaClient(
            server=self.host,
            share=self.database,
            username=self.user,
            domain=self.domain,
            port=self.port,
            # does not work without \n on smbclient --version Version 4.7.1
            password=self.password + "\n",
        )

    def is_dir(self, top, item) -> bool:
        return "D" in item[1]

    def get_name(self, item) -> str:
        return item[0]

    def download_file(self, remote_file_path: str, local_file_path: str) -> None:
        self.client.run(remote_file_path, local_file_path)
        log.info(f"Successfully download_file {remote_file_path} remote SMB to {local_file_path}")

    def remove_file(self, remote_file_path: str) -> None:
        """
        Remove remote file

        :param client:
        :type client: SambaClient
        :param remote_file_path:
        :return:
        """
        self.client.unlink(remote_file_path)
        log.info(f"Successfully removed file {remote_file_path} from SMB")

    def mkdir(self, path: str) -> None:
        self.client.mkdir(path)
        log.info(f"Successfully created directory {path}")

    def upload_file(self, local_file_path: str, remote_file_path: str, *args, **kwargs) -> None:
        self.client.run(local_file_path, remote_file_path)

    def path_exists(self, path: str) -> bool:
        return self.client.exists(path)

    def rename(self, source: str, target: str) -> None:
        self.client.rename(source, target)
        log.info(f"Successfully renamed file {source} to {target}")

    def rmdir(self, path: str, recursive: bool) -> None:
        for file in self.client.listdir():
            self.client.unlink(file)
        self.client.rmdir(path)
        log.info(f"Successfully removed path {path} from SMB")

    def _listdir(self, path) -> List:
        return self.client.lsdir(path)
