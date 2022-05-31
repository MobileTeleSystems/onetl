from __future__ import annotations

import os
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path, PurePosixPath
from stat import S_ISDIR, S_ISREG

from paramiko import AutoAddPolicy, ProxyCommand, SSHClient, SSHConfig
from paramiko.sftp_attr import SFTPAttributes
from paramiko.sftp_client import SFTPClient

from onetl.connection.file_connection.file_connection import FileConnection

SSH_CONFIG_PATH = Path("~/.ssh/config").expanduser().resolve()

log = getLogger(__name__)


@dataclass(frozen=True)
class SFTP(FileConnection):
    """Class for SFTP file connection.

    Parameters
    ----------
    host : str
        Host of sftp source. For example: ``0001testadviat04.msk.mts.ru``
    port : int, default: ``22``
        Port of sftp source
    user : str
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

        from onetl.connection import SFTP

        sftp = SFTP(
            host="0001testadviat04.msk.mts.ru",
            user="sa0000sphrsftptest",
            password="*****",
        )
    """

    port: int = 22
    key_file: str | None = None
    timeout: int = 10
    host_key_check: bool = False
    compress: bool = True

    def path_exists(self, path: os.PathLike | str) -> bool:
        try:
            self.client.stat(os.fspath(path))
            return True
        except FileNotFoundError:
            return False

    def _get_client(self) -> SFTPClient:
        host_proxy, key_file = self._parse_user_ssh_config()

        client = SSHClient()
        client.load_system_host_keys()
        if not self.host_key_check:
            # Default is RejectPolicy
            client.set_missing_host_key_policy(AutoAddPolicy())

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

    def _is_client_closed(self) -> bool:
        return not self._client.sock or self._client.sock.closed

    def _close_client(self) -> None:
        self._client.close()

    def _parse_user_ssh_config(self) -> tuple[str | None, str | None]:
        host_proxy = None
        key_file = self.key_file

        if SSH_CONFIG_PATH.exists() and SSH_CONFIG_PATH.is_file():
            ssh_conf = SSHConfig()
            ssh_conf.parse(SSH_CONFIG_PATH.read_text())
            host_info = ssh_conf.lookup(self.host) or {}
            if host_info.get("proxycommand"):
                host_proxy = ProxyCommand(host_info.get("proxycommand"))

            if not (self.password or key_file) and host_info.get("identityfile"):
                key_file = host_info.get("identityfile")[0]

        return host_proxy, key_file

    def _mkdir(self, path: os.PathLike | str) -> None:
        try:
            self.client.stat(os.fspath(path))
        except Exception:
            for parent in reversed(PurePosixPath(path).parents):
                try:  # noqa: WPS505
                    self.client.stat(os.fspath(parent))
                except Exception:
                    self.client.mkdir(os.fspath(parent))

            self.client.mkdir(os.fspath(path))

    def _upload_file(self, local_file_path: os.PathLike | str, remote_file_path: os.PathLike | str) -> None:
        self.client.put(os.fspath(local_file_path), os.fspath(remote_file_path))

    def _rename(self, source: os.PathLike | str, target: os.PathLike | str) -> None:
        self.client.rename(os.fspath(source), os.fspath(target))

    def _download_file(self, remote_file_path: os.PathLike | str, local_file_path: os.PathLike | str) -> None:
        self.client.get(os.fspath(remote_file_path), os.fspath(local_file_path))

    def _rmdir(self, path: os.PathLike | str) -> None:
        self.client.rmdir(os.fspath(path))

    def _remove_file(self, remote_file_path: os.PathLike | str) -> None:
        self.client.remove(os.fspath(remote_file_path))

    def _listdir(self, path: os.PathLike | str) -> list:
        return self.client.listdir_attr(os.fspath(path))

    def _is_dir(self, path: os.PathLike | str) -> bool:
        stat: SFTPAttributes = self.client.stat(os.fspath(path))
        return S_ISDIR(stat.st_mode)

    def _is_file(self, path: os.PathLike | str) -> bool:
        stat: SFTPAttributes = self.client.stat(os.fspath(path))
        return S_ISREG(stat.st_mode)

    def _get_stat(self, path: os.PathLike | str) -> SFTPAttributes:
        return self.client.stat(os.fspath(path))

    def _get_item_name(self, item: SFTPAttributes) -> str:
        return item.filename

    def _is_item_dir(self, top: os.PathLike | str, item: SFTPAttributes) -> bool:
        return S_ISDIR(item.st_mode)

    def _is_item_file(self, top: os.PathLike | str, item: SFTPAttributes) -> bool:
        return S_ISREG(item.st_mode)

    def _get_item_stat(self, top: os.PathLike | str, item: SFTPAttributes) -> SFTPAttributes:
        return item

    def _read_text(self, path: os.PathLike | str, encoding: str, **kwargs) -> str:
        with self.client.open(os.fspath(path), mode="r", **kwargs) as file:
            return file.read().decode(encoding)

    def _read_bytes(self, path: os.PathLike | str, **kwargs) -> bytes:
        with self.client.open(os.fspath(path), mode="r", **kwargs) as file:
            return file.read()

    def _write_text(self, path: os.PathLike | str, content: str, encoding: str, **kwargs) -> None:
        with self.client.open(os.fspath(path), mode="w", **kwargs) as file:
            file.write(content.encode(encoding))

    def _write_bytes(self, path: os.PathLike | str, content: bytes, **kwargs) -> None:
        with self.client.open(os.fspath(path), mode="w", **kwargs) as file:
            file.write(content)
