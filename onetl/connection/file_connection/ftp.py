# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import ftplib  # noqa: S402  # nosec
import os
import textwrap
from logging import getLogger
from typing import Optional

from etl_entities.instance import Host

try:
    from pydantic.v1 import SecretStr
except (ImportError, AttributeError):
    from pydantic import SecretStr  # type: ignore[no-redef, assignment]

from onetl.base import PathStatProtocol
from onetl.connection.file_connection.file_connection import FileConnection
from onetl.connection.file_connection.mixins.rename_dir_mixin import RenameDirMixin
from onetl.hooks import slot, support_hooks
from onetl.impl import LocalPath, RemotePath
from onetl.impl.remote_path_stat import RemotePathStat

try:
    from ftputil import FTPHost
    from ftputil import session as ftp_session
except (ImportError, NameError) as e:
    raise ImportError(
        textwrap.dedent(
            """
            Cannot import module "ftputil".

            Since onETL v0.7.0 you should install package as follows:
                pip install onetl[ftp]

            or
                pip install onetl[files]
            """,
        ).strip(),
    ) from e

log = getLogger(__name__)


@support_hooks
class FTP(FileConnection, RenameDirMixin):
    """FTP file connection. |support_hooks|

    Based on `FTPUtil library <https://pypi.org/project/ftputil/>`_.

    .. warning::

        Since onETL v0.7.0 to use FTP connector you should install package as follows:

        .. code:: bash

            pip install onetl[ftp]

            # or
            pip install onetl[files]

        See :ref:`install-files` installation instruction for more details.

    .. versionadded:: 0.1.0

    Parameters
    ----------
    host : str
        Host of FTP source. For example: ``ftp.domain.com``

    port : int, default: ``21``
        Port of FTP source

    user : str, default: ``None``
        User, which have access to the file source. For example: ``someuser``.

        ``None`` means that the user is anonymous.

    password : str, default: ``None``
        Password for file source connection.

        ``None`` means that the user is anonymous.

    Examples
    --------

    Create and check FTP connection:

    .. code:: python

        from onetl.connection import FTP

        ftp = FTP(
            host="ftp.domain.com",
            user="someuser",
            password="*****",
        ).check()
    """

    host: Host
    port: int = 21
    user: Optional[str] = None
    password: Optional[SecretStr] = None

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}"

    def __str__(self):
        return f"{self.__class__.__name__}[{self.host}:{self.port}]"

    @slot
    def path_exists(self, path: os.PathLike | str) -> bool:
        return self.client.path.exists(os.fspath(path))

    def _get_client(self) -> FTPHost:
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
            self.password.get_secret_value() if self.password else None,
            session_factory=session_factory,
        )

    def _is_client_closed(self, client: FTPHost) -> bool:
        return client.closed

    def _close_client(self, client: FTPHost) -> None:
        client.close()

    def _remove_dir(self, path: RemotePath) -> None:
        self.client.rmdir(os.fspath(path))

    def _upload_file(self, local_file_path: LocalPath, remote_file_path: RemotePath) -> None:
        self.client.upload(os.fspath(local_file_path), os.fspath(remote_file_path))

    def _rename_file(self, source: RemotePath, target: RemotePath) -> None:
        self.client.rename(os.fspath(source), os.fspath(target))

    _rename_dir = _rename_file

    def _download_file(self, remote_file_path: RemotePath, local_file_path: LocalPath) -> None:
        self.client.download(os.fspath(remote_file_path), os.fspath(local_file_path))

    def _remove_file(self, remote_file_path: RemotePath) -> None:
        self.client.remove(os.fspath(remote_file_path))

    def _create_dir(self, path: RemotePath) -> None:
        self.client.makedirs(os.fspath(path), exist_ok=True)

    def _scan_entries(self, path: RemotePath) -> list[str]:
        return self.client.listdir(os.fspath(path))

    def _is_dir(self, path: RemotePath) -> bool:
        return self.client.path.isdir(os.fspath(path))

    def _is_file(self, path: RemotePath) -> bool:
        return self.client.path.isfile(os.fspath(path))

    def _get_stat(self, path: RemotePath) -> PathStatProtocol:
        if path == RemotePath("/"):
            # FTP does not allow to call stat on root directory, do nothing
            return RemotePathStat()

        # underlying FTP client already return `os.stat_result`-like class`
        return self.client.stat(os.fspath(path))

    def _read_text(self, path: RemotePath, encoding: str, **kwargs) -> str:
        with self.client.open(os.fspath(path), mode="r", encoding=encoding, **kwargs) as file:
            return file.read()

    def _read_bytes(self, path: RemotePath, **kwargs) -> bytes:
        with self.client.open(os.fspath(path), mode="rb", **kwargs) as file:
            return file.read()

    def _write_text(self, path: RemotePath, content: str, encoding: str, **kwargs) -> None:
        with self.client.open(os.fspath(path), mode="w", encoding=encoding, **kwargs) as file:
            file.write(content)

    def _write_bytes(self, path: RemotePath, content: bytes, **kwargs) -> None:
        with self.client.open(os.fspath(path), mode="wb", **kwargs) as file:
            file.write(content)

    def _extract_name_from_entry(self, entry: str) -> str:
        return entry

    def _is_dir_entry(self, top: RemotePath, entry: str) -> bool:
        return self._is_dir(top / entry)

    def _is_file_entry(self, top: RemotePath, entry: str) -> bool:
        return self._is_file(top / entry)

    def _extract_stat_from_entry(self, top: RemotePath, entry: str) -> PathStatProtocol:
        return self._get_stat(top / entry)
