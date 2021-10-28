import os
import posixpath
from dataclasses import dataclass
from logging import getLogger
from stat import S_ISDIR
from typing import List, Optional

import paramiko

from onetl.connection.file_connection.file_connection import FileConnection

log = getLogger(__name__)


@dataclass(frozen=True)
class SFTP(FileConnection):
    """Class for SFTP file connection.

    Parameters
    ----------
    host : str
        Host of sftp source. For example: ``0001testadviat04.msk.mts.ru``
    port : int, optional, default: ``22``
        Port of sftp source
    user : str, default: ``None``
        User, which have access to the file source. For example: ``sa0000sphrsftptest``
    password : str, default: ``None``
        Password for file source connection
    key_file : str, default: ``None``
        the filename of optional private key(s) and/or certs to try for authentication
    timeout : int, default: ``10``
        How long to wait for the server to send data before giving up.
    host_key_check : bool, default: ``False``
        set to True to enable searching for discoverable private key files in ``~/.ssh/``
    compress : bool, default: ``True``
        Set to True to turn on compression

    Examples
    --------

    SFTP file connection initialization

    .. code::

        from onetl.connection.file_connection import SFTP

        sftp = SFTP(
            host="0001testadviat04.msk.mts.ru",
            user="sa0000sphrsftptest",
            password="*****",
        )
    """

    port: int = 22
    key_file: Optional[str] = None
    timeout: int = 10
    host_key_check: bool = False
    compress: bool = True

    def get_client(self) -> "paramiko.sftp_client.SFTPClient":
        log.info("creating ssh client")

        host_proxy, key_file = self.parse_user_ssh_config(self.key_file)

        client = paramiko.SSHClient()
        client.load_system_host_keys()
        if not self.host_key_check:
            # Default is RejectPolicy
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if self.password:
            client.connect(
                hostname=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                timeout=self.timeout,
                compress=self.compress,
                sock=host_proxy,
            )
        else:
            client.connect(
                hostname=self.host,
                port=self.port,
                username=self.user,
                key_filename=key_file,
                timeout=self.timeout,
                compress=self.compress,
                sock=host_proxy,
            )

        return client.open_sftp()

    def is_dir(self, top, item) -> bool:
        return S_ISDIR(item.st_mode)

    def get_name(self, item) -> str:
        return item.filename

    def download_file(self, remote_file_path: str, local_file_path: str) -> None:
        self.client.get(remote_file_path, local_file_path)
        log.info(f"Successfully download file {remote_file_path} remote SFTP to {local_file_path}")

    def remove_file(self, remote_file_path: str) -> None:
        self.client.remove(remote_file_path)
        log.info(f"Successfully removed file {remote_file_path}")

    def path_exists(self, path: str) -> bool:
        try:
            self.client.stat(path)
            return True
        except FileNotFoundError:
            return False

    def mkdir(self, path: str) -> None:
        try:
            self.client.stat(path)
        except Exception:
            abs_path = "/"
            for directory in path.strip(posixpath.sep).split(posixpath.sep):
                abs_path = posixpath.join(abs_path, directory)
                try:  # noqa: WPS505
                    self.client.stat(abs_path)
                except Exception:
                    self.client.mkdir(abs_path)
        log.info(f"Successfully created dir {path}")

    def upload_file(self, local_file_path: str, remote_file_path: str, *args, **kwargs) -> None:
        self.client.put(local_file_path, remote_file_path)
        log.info(f"Successfully uploaded _file from {local_file_path} to remote SFTP {remote_file_path}")

    def rename(self, source: str, target: str) -> None:
        self.client.rename(source, target)
        log.info(f"Successfully renamed file {source} to {target}")

    def parse_user_ssh_config(self, key_file):
        user_ssh_config_filename = os.path.expanduser("~/.ssh/config")
        host_proxy = None
        if os.path.isfile(user_ssh_config_filename):
            ssh_conf = paramiko.SSHConfig()
            ssh_conf.parse(open(user_ssh_config_filename))  # NOQA WPS515
            host_info = ssh_conf.lookup(self.host) or {}
            if host_info.get("proxycommand"):
                host_proxy = paramiko.ProxyCommand(host_info.get("proxycommand"))

            if not (self.password or key_file) and host_info.get("identityfile"):
                key_file = host_info.get("identityfile")[0]  # NOQA WPS220

        return host_proxy, key_file

    def _listdir(self, path: str) -> List:
        return self.client.listdir_attr(path)
