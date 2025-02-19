# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import contextlib
import os
import textwrap
from logging import getLogger
from stat import S_ISDIR, S_ISREG
from typing import Optional

from etl_entities.instance import Host

try:
    from pydantic.v1 import FilePath, SecretStr
except (ImportError, AttributeError):
    from pydantic import FilePath, SecretStr  # type: ignore[no-redef, assignment]

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.connection.file_connection.mixins.rename_dir_mixin import RenameDirMixin
from onetl.hooks import slot, support_hooks
from onetl.impl import LocalPath, RemotePath

try:
    from paramiko import ProxyCommand, SSHClient, SSHConfig, WarningPolicy
    from paramiko.sftp_attr import SFTPAttributes
    from paramiko.sftp_client import SFTPClient
    from paramiko.ssh_exception import ConfigParseError
except (ImportError, NameError) as e:
    raise ImportError(
        textwrap.dedent(
            """
            Cannot import module "paramiko".

            Since onETL v0.7.0 you should install package as follows:
                pip install onetl[sftp]

            or
                pip install onetl[files]
            """,
        ).strip(),
    ) from e

SSH_CONFIG_PATH = LocalPath("~/.ssh/config").expanduser().resolve()

log = getLogger(__name__)


@support_hooks
class SFTP(FileConnection, RenameDirMixin):
    """SFTP file connection. |support_hooks|

    Based on `Paramiko library <https://pypi.org/project/paramiko/>`_.

    .. warning::

        Since onETL v0.7.0 to use SFTP connector you should install package as follows:

        .. code:: bash

            pip install onetl[s3]

            # or
            pip install onetl[files]

        See :ref:`install-files` installation instruction for more details.

    .. versionadded:: 0.1.0

    Parameters
    ----------
    host : str
        Host of SFTP source. For example: ``192.168.1.19``

    port : int, default: ``22``
        Port of SFTP source

    user : str
        User, which have access to the file source. For example: ``someuser``

    password : str, default: ``None``
        Password for file source connection

    key_file : str, default: ``None``
        the filename of optional private key(s) and/or certs to try for authentication

    timeout : int, default: ``10``
        How long to wait for the server to send data before giving up

    host_key_check : bool, default: ``False``
        set to True to enable searching for discoverable private key files in ``~/.ssh/``

    compress : bool, default: ``True``
        Set to True to turn on compression

    Examples
    --------

    Create and check SFTP connection:

    .. code:: python

        from onetl.connection import SFTP

        sftp = SFTP(
            host="192.168.1.19",
            user="someuser",
            password="*****",
        ).check()
    """

    host: Host
    port: int = 22
    user: Optional[str] = None
    password: Optional[SecretStr] = None
    key_file: Optional[FilePath] = None
    timeout: int = 10
    host_key_check: bool = False
    compress: bool = True

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}"

    def __str__(self):
        return f"{self.__class__.__name__}[{self.host}:{self.port}]"

    @slot
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
            client.set_missing_host_key_policy(WarningPolicy())  # noqa: S507

        client.connect(
            hostname=self.host,
            port=self.port,
            username=self.user,
            password=self.password.get_secret_value() if self.password else None,
            key_filename=key_file,
            timeout=self.timeout,
            compress=self.compress,
            sock=host_proxy,
        )

        return client.open_sftp()

    def _is_client_closed(self, client: SFTPClient) -> bool:
        return not client.sock or client.sock.closed

    def _close_client(self, client: SFTPClient) -> None:
        client.close()

    def _parse_user_ssh_config(self) -> tuple[str | None, str | None]:
        host_proxy = None
        key_file = os.fspath(self.key_file) if self.key_file else None

        if not SSH_CONFIG_PATH.exists() or not SSH_CONFIG_PATH.is_file():
            return host_proxy, key_file

        try:
            ssh_conf = SSHConfig()
            ssh_conf.parse(SSH_CONFIG_PATH.read_text())
            host_info = ssh_conf.lookup(self.host) or {}
            if host_info.get("proxycommand"):
                host_proxy = ProxyCommand(host_info.get("proxycommand"))

            if not (self.password or key_file) and host_info.get("identityfile"):
                key_file = host_info.get("identityfile")[0]
        except ConfigParseError:
            log.exception("Failed to parse SSH config")

        return host_proxy, key_file

    def _create_dir(self, path: RemotePath) -> None:
        try:
            self.client.stat(os.fspath(path))
        except Exception:
            for parent in reversed(path.parents):
                try:  # noqa: WPS505
                    self.client.stat(os.fspath(parent))
                except Exception:
                    self.client.mkdir(os.fspath(parent))

            self.client.mkdir(os.fspath(path))

    def _upload_file(self, local_file_path: RemotePath, remote_file_path: RemotePath) -> None:
        self.client.put(os.fspath(local_file_path), os.fspath(remote_file_path))

    def _rename_file(self, source: RemotePath, target: RemotePath) -> None:
        with contextlib.suppress(OSError):
            self.client.posix_rename(os.fspath(source), os.fspath(target))
            return

        # posix rename extension is not supported by server
        # if OSError was caused by permissions error, client.rename will raise this exception again
        self.client.rename(os.fspath(source), os.fspath(target))

    _rename_dir = _rename_file

    def _download_file(self, remote_file_path: RemotePath, local_file_path: RemotePath) -> None:
        self.client.get(os.fspath(remote_file_path), os.fspath(local_file_path))

    def _remove_dir(self, path: RemotePath) -> None:
        self.client.rmdir(os.fspath(path))

    def _remove_file(self, remote_file_path: RemotePath) -> None:
        self.client.remove(os.fspath(remote_file_path))

    def _scan_entries(self, path: RemotePath) -> list[SFTPAttributes]:
        return self.client.listdir_attr(os.fspath(path))

    def _is_dir(self, path: RemotePath) -> bool:
        stat: SFTPAttributes = self.client.stat(os.fspath(path))
        return S_ISDIR(stat.st_mode)

    def _is_file(self, path: RemotePath) -> bool:
        stat: SFTPAttributes = self.client.stat(os.fspath(path))
        return S_ISREG(stat.st_mode)

    def _get_stat(self, path: RemotePath) -> SFTPAttributes:
        # underlying SFTP client already return `os.stat_result`-like class
        return self.client.stat(os.fspath(path))

    def _extract_name_from_entry(self, entry: SFTPAttributes) -> str:
        return entry.filename

    def _is_dir_entry(self, top: RemotePath, entry: SFTPAttributes) -> bool:
        return S_ISDIR(entry.st_mode)

    def _is_file_entry(self, top: RemotePath, entry: SFTPAttributes) -> bool:
        return S_ISREG(entry.st_mode)

    def _extract_stat_from_entry(self, top: RemotePath, entry: SFTPAttributes) -> SFTPAttributes:
        return entry

    def _read_text(self, path: RemotePath, encoding: str, **kwargs) -> str:
        with self.client.open(os.fspath(path), mode="r", **kwargs) as file:
            return file.read().decode(encoding)

    def _read_bytes(self, path: RemotePath, **kwargs) -> bytes:
        with self.client.open(os.fspath(path), mode="r", **kwargs) as file:
            return file.read()

    def _write_text(self, path: RemotePath, content: str, encoding: str, **kwargs) -> None:
        with self.client.open(os.fspath(path), mode="w", **kwargs) as file:
            file.write(content.encode(encoding))

    def _write_bytes(self, path: RemotePath, content: bytes, **kwargs) -> None:
        with self.client.open(os.fspath(path), mode="w", **kwargs) as file:
            file.write(content)
