from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from logging import getLogger
from typing import List, Tuple

from smbclient import SambaClient

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.impl import RemoteFileStat

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
        return self.client.exists(path)

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

    def _rename(self, source: os.PathLike | str, target: os.PathLike | str) -> None:
        self.client.rename(source, target)

    def _download_file(self, remote_file_path: os.PathLike | str, local_file_path: os.PathLike | str) -> None:
        self.client.run(remote_file_path, local_file_path)

    def _remove_file(self, remote_file_path: os.PathLike | str) -> None:
        self.client.unlink(remote_file_path)

    def _mkdir(self, path: os.PathLike | str) -> None:
        self.client.mkdir(path)

    def _rmdir(self, path: os.PathLike | str) -> None:
        self.client.rmdir(os.fspath(path))

    def _upload_file(self, local_file_path: os.PathLike | str, remote_file_path: os.PathLike | str) -> None:
        self.client.run(local_file_path, remote_file_path)

    def _listdir(self, path: os.PathLike | str) -> list:
        return self.client.lsdir(path)

    def _is_dir(self, path: os.PathLike | str) -> bool:
        return self.client.self.client.isdir(path)

    def _is_file(self, path: os.PathLike | str) -> bool:
        return self.client.self.client.isfile(path)

    def _get_stat(self, path: os.PathLike | str) -> RemoteFileStat:
        item: ItemType = next(self.client.self.client.glob(path))
        return RemoteFileStat(st_size=item[2], st_mtime=item[3].timestamp())

    def _get_item_name(self, item: ItemType) -> str:
        return item[0]

    def _is_item_dir(self, top: os.PathLike | str, item: ItemType) -> bool:
        return "D" in item[1]

    def _is_item_file(self, top: os.PathLike | str, item: ItemType) -> bool:
        return not self._is_item_dir(top, item)

    def _get_item_stat(self, top: os.PathLike | str, item: ItemType) -> RemoteFileStat:
        return RemoteFileStat(st_size=item[2], st_mtime=item[3].timestamp())
