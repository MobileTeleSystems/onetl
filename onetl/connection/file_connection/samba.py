from __future__ import annotations

import os
import stat
from dataclasses import dataclass
from datetime import datetime
from logging import getLogger
from typing import List, Tuple

from smbclient import SambaClient

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.impl import RemotePath, RemotePathStat
from onetl.impl.local_path import LocalPath

log = getLogger(__name__)

# Result type of listdir or glob methods is: [(filename, modes, size, date), ...]
ItemType = List[Tuple[str, int, int, datetime]]


@dataclass(frozen=True)
class Samba(FileConnection):
    """Class for Samba file connection.

    Parameters
    ----------
    host : str
        Host of samba source. For example: ``msk.mts.ru``
    port : int, default: ``445``
        Port of samba source
    user : str
        User, which have access to the file source. For example: ``sa0000techgen``
    password : str, default: ``None``
        Password for file source connection
    domain : str, default: ``None``
        A Samba domain member is a Linux machine joined to a domain that is running Samba
        and does not provide domain services,
        such as an NT4 primary domain controller (PDC) or Active Directory (AD) domain controller (DC)
    schema : str, default: ``None``
        Secure File Server Share path. For example: ``MSK``

    Examples
    --------

    Samba file connection initialization

    .. code::

        from onetl.connection import Samba

        samba = Samba(
            host="msk.mts.ru",
            user="sa0000techgen",
            password="*****",
            schema="MSK",
        )
    """

    port: int = 445
    domain: str | None = None
    schema: str | None = None

    def path_exists(self, path: os.PathLike | str) -> bool:
        return self.client.exists(os.fspath(path))

    def _get_client(self) -> SambaClient:
        return SambaClient(
            server=self.host,
            share=self.schema,
            username=self.user,
            domain=self.domain,
            port=self.port,
            # does not work without \n on smbclient --version Version 4.7.1
            password=self.password + "\n",
        )

    def _is_client_closed(self) -> bool:
        # Underlying client does not have `closed` attribute
        return False

    def _close_client(self) -> None:
        self._client.close()

    def _rename(self, source: RemotePath, target: RemotePath) -> None:
        self.client.rename(os.fspath(source), os.fspath(target))

    def _download_file(self, remote_file_path: RemotePath, local_file_path: LocalPath) -> None:
        self.client.run(os.fspath(remote_file_path), os.fspath(local_file_path))

    def _remove_file(self, remote_file_path: RemotePath) -> None:
        self.client.unlink(os.fspath(remote_file_path))

    def _mkdir(self, path: RemotePath) -> None:
        self.client.mkdir(os.fspath(path))

    def _rmdir(self, path: RemotePath) -> None:
        self.client.rmdir(os.fspath(path))

    def _upload_file(self, local_file_path: LocalPath, remote_file_path: RemotePath) -> None:
        self.client.run(os.fspath(local_file_path), os.fspath(remote_file_path))

    def _listdir(self, path: RemotePath) -> list:
        return self.client.lsdir(os.fspath(path))

    def _is_dir(self, path: RemotePath) -> bool:
        return self.client.self.client.isdir(os.fspath(path))

    def _is_file(self, path: RemotePath) -> bool:
        return self.client.self.client.isfile(os.fspath(path))

    def _get_stat(self, path: RemotePath) -> RemotePathStat:
        item: ItemType = next(self.client.glob(os.fspath(path)))
        return RemotePathStat(st_size=item[2], st_mtime=item[3].timestamp())

    def _get_item_name(self, item: ItemType) -> str:
        return item[0]

    def _is_item_dir(self, top: RemotePath, item: ItemType) -> bool:
        return "D" in item[1]

    def _is_item_file(self, top: RemotePath, item: ItemType) -> bool:
        return not self._is_item_dir(top, item)

    def _get_item_stat(self, top: RemotePath, item: ItemType) -> RemotePathStat:
        st_mode = stat.S_IFDIR if "D" in item[1] else stat.S_IFREG
        return RemotePathStat(st_size=item[2], st_mtime=item[3].timestamp(), st_mode=st_mode)

    def _read_text(self, path: RemotePath, encoding: str, **kwargs) -> str:
        with self.client.open(os.fspath(path), mode="rb", **kwargs) as file:
            return file.read().decode(encoding)

    def _read_bytes(self, path: RemotePath, **kwargs) -> bytes:
        with self.client.open(os.fspath(path), mode="rb", **kwargs) as file:
            return file.read()

    def _write_text(self, path: RemotePath, content: str, encoding: str, **kwargs) -> None:
        with self.client.open(os.fspath(path), mode="wb", **kwargs) as file:
            file.write(content.encode(encoding))

    def _write_bytes(self, path: RemotePath, content: bytes, **kwargs) -> None:
        with self.client.open(os.fspath(path), mode="wb", **kwargs) as file:
            file.write(content)
