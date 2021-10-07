import os
import posixpath
from dataclasses import dataclass
from logging import getLogger
from stat import S_ISDIR
from typing import List

import paramiko

from onetl.connection.file_connection.file_connection import FileConnection

log = getLogger(__name__)


@dataclass(frozen=True)
class SFTP(FileConnection):
    port: int = 22

    def get_client(self) -> "paramiko.sftp_client.SFTPClient":
        log.info("creating ssh client")

        key_file = self.extra.get("key_file")
        timeout = int(self.extra.get("timeout", 10))
        compress = True
        no_host_key_check = self.extra.get("no_host_key_check", True)

        if self.extra.get("compress", "").lower() == "false":
            compress = False

        host_proxy, key_file = self.parse_user_ssh_config(key_file)

        client = paramiko.SSHClient()
        client.load_system_host_keys()
        if no_host_key_check:
            # Default is RejectPolicy
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if self.password:
            client.connect(
                hostname=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                timeout=timeout,
                compress=compress,
                sock=host_proxy,
            )
        else:
            client.connect(
                hostname=self.host,
                port=self.port,
                username=self.user,
                key_filename=key_file,
                timeout=timeout,
                compress=compress,
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

    def mk_dir(self, path: str) -> None:
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

    def rmdir(self, path: str, recursive: bool) -> None:
        self.client.rmdir(path)
        log.info(f"Successfully removed path {path}")

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
