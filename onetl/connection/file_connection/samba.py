from __future__ import annotations

import os
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path

from smbclient import SambaClient

from onetl.connection.file_connection.file_connection import FileConnection

log = getLogger(__name__)


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

    def get_client(self) -> SambaClient:

        return SambaClient(
            server=self.host,
            share=self.schema,
            username=self.user,
            domain=self.domain,
            port=self.port,
            # does not work without \n on smbclient --version Version 4.7.1
            password=self.password + "\n",
        )

    def is_dir(self, top, item) -> bool:
        return "D" in item[1]

    def get_name(self, item) -> Path:
        return Path(item[0])

    def path_exists(self, path: os.PathLike | str) -> bool:
        return self.client.exists(path)

    def _rename(self, source: os.PathLike | str, target: os.PathLike | str) -> None:
        self.client.rename(source, target)

    def _listdir(self, path: os.PathLike | str) -> list:
        return self.client.lsdir(path)

    def _download_file(self, remote_file_path: os.PathLike | str, local_file_path: os.PathLike | str) -> None:
        self.client.run(remote_file_path, local_file_path)

    def _remove_file(self, remote_file_path: os.PathLike | str) -> None:
        self.client.unlink(remote_file_path)

    def _mkdir(self, path: os.PathLike | str) -> None:
        self.client.mkdir(path)

    def _upload_file(self, local_file_path: os.PathLike | str, remote_file_path: os.PathLike | str) -> None:
        self.client.run(local_file_path, remote_file_path)
