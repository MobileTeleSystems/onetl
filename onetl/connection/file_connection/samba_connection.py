from dataclasses import dataclass
from logging import getLogger

from smbclient import SambaClient

from onetl.connection.file_connection.file_connection import FileConnection

log = getLogger(__name__)


@dataclass(frozen=True)
class Samba(FileConnection):
    """
    Airflow connection example:
    Conn Id:    samba
    Conn Type:  samba
    Host:       0000mskrwdc01
    Schema:     msk
    User:      username
    Password:   pass
    Extra:      {"domain": "ADMSK"}
    """
    port: int = 445

    def get_client(self) -> SambaClient:
        kw = self.extra.copy()
        kw.update(
            server=self.host,
            share=self.database,
            username=self.user,
        )
        if self.password:
            # does not work without \n on smbclient --version Version 4.7.1
            kw['password'] = self.password + '\n'

        return SambaClient(**kw)

    def is_dir(self, top, item):
        return 'D' in item[1]

    def get_name(self, item):
        return item[0]

    def download_file(self, remote_file_path: str, local_file_path: str) -> None:
        self.client.download(remote_file_path, local_file_path)
        log.info(f'Successfully download_file {remote_file_path} remote SMB to {local_file_path}')

    def remove_file(self, remote_file_path):
        """
        Remove remote file

        :param client:
        :type client: SambaClient
        :param remote_file_path:
        :return:
        """
        self.client.remove(remote_file_path)
        log.info(f'Successfully removed file {remote_file_path} from SMB')

    def _listdir(self, path):
        return self.client.lsdir(path)
