# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import stat
import textwrap
from io import BytesIO
from logging import getLogger
from pathlib import Path
from typing import Optional, Union

from etl_entities.instance import Host
from typing_extensions import Literal

try:
    from pydantic.v1 import SecretStr, validator
except (ImportError, AttributeError):
    from pydantic import SecretStr, validator  # type: ignore[no-redef, assignment]

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.hooks import slot, support_hooks
from onetl.impl import LocalPath, RemotePath, RemotePathStat

try:
    from smb.smb_structs import OperationFailure
    from smb.SMBConnection import SMBConnection
except (ImportError, NameError) as e:
    raise ImportError(
        textwrap.dedent(
            """
            Cannot import module "pysmb".

            You should install package as follows:
                pip install onetl[samba]

            or
                pip install onetl[files]
            """,
        ).strip(),
    ) from e


log = getLogger(__name__)


@support_hooks
class Samba(FileConnection):
    """Samba file connection. |support_hooks|

    Based on `pysmb library <https://pypi.org/project/pysmb/>`_.

    .. versionadded:: 0.9.4

    .. warning::

        To use Samba connector you should install package as follows:

        .. code:: bash

            pip install onetl[samba]

            # or
            pip install onetl[files]

        See :ref:`install-files` installation instruction for more details.

    Parameters
    ----------
    host : str
        Host of Samba source. For example: ``mydomain.com``.

    share : str
        The name of the share on the Samba server.

    protocol : str, default: ``SMB``
        The protocol to use for the connection. Either ``SMB`` or ``NetBIOS``.
        Affects the default port and the `is_direct_tcp` flag in `SMBConnection`.

    port : int, default: 445
        Port of Samba source.

    domain : str, default: ``
        Domain name for the Samba connection. Empty strings means use ``host`` as domain name.

    auth_type : str, default: ``NTLMv2``
        The authentication type to use. Either ``NTLMv2`` or ``NTLMv1``.
        Affects the `use_ntlm_v2` flag in `SMBConnection`.

    user : str, default: None
        User, which have access to the file source. Can be `None` for anonymous connection.

    password : str, default: None
        Password for file source connection. Can be `None` for anonymous connection.

    Examples
    --------

    Create and check Samba connection:

    .. code:: python

        from onetl.connection import Samba

        samba = Samba(
            host="mydomain.com",
            share="share_name",
            protocol="SMB",
            port=445,
            user="user",
            password="password",
        ).check()
    """

    host: Host
    share: str
    protocol: Union[Literal["SMB"], Literal["NetBIOS"]] = "SMB"
    port: Optional[int] = None
    domain: Optional[str] = ""
    auth_type: Union[Literal["NTLMv1"], Literal["NTLMv2"]] = "NTLMv2"
    user: Optional[str] = None
    password: Optional[SecretStr] = None

    @property
    def instance_url(self) -> str:
        return f"smb://{self.host}:{self.port}/{self.share}"

    def __str__(self):
        return f"{self.__class__.__name__}[{self.host}:{self.port}/{self.share}]"

    @slot
    def check(self):
        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()
        try:
            available_shares = {share.name for share in self.client.listShares()}
            if self.share in available_shares:
                log.info("|%s| Connection is available.", self.__class__.__name__)
            else:
                log.error(
                    "|%s| Share %r not found among existing shares %r",
                    self.__class__.__name__,
                    self.share,
                    available_shares,
                )
                raise ConnectionError("Failed to connect to the Samba server.")
        except Exception as exc:
            log.exception("|%s| Connection is unavailable", self.__class__.__name__)
            raise RuntimeError("Connection is unavailable") from exc

        return self

    @slot
    def path_exists(self, path: os.PathLike | str) -> bool:
        try:
            self.client.getAttributes(self.share, os.fspath(path))
            return True
        except OperationFailure:
            return False

    def _scan_entries(self, path: RemotePath) -> list:
        if self._is_dir(path):
            return [
                entry
                for entry in self.client.listPath(
                    self.share,
                    os.fspath(path),
                )
                if entry.filename not in {".", ".."}  # Filter out '.' and '..'
            ]
        return [self.client.getAttributes(self.share, os.fspath(path))]

    def _extract_name_from_entry(self, entry) -> str:
        return entry.filename

    def _is_dir_entry(self, top: RemotePath, entry) -> bool:
        return entry.isDirectory

    def _is_file_entry(self, top: RemotePath, entry) -> bool:
        return not entry.isDirectory

    def _extract_stat_from_entry(self, top: RemotePath, entry) -> RemotePathStat:
        if entry.isDirectory:
            return RemotePathStat(st_mode=stat.S_IFDIR)

        return RemotePathStat(
            st_size=entry.file_size,
            st_mtime=entry.last_write_time,
            st_uid=entry.filename,
        )

    def _get_client(self) -> SMBConnection:
        is_direct_tcp = self.protocol == "SMB"
        use_ntlm_v2 = self.auth_type == "NTLMv2"
        conn = SMBConnection(
            username=self.user,
            password=self.password.get_secret_value() if self.password else None,
            my_name="onetl",
            remote_name=self.host,
            domain=self.domain,
            use_ntlm_v2=use_ntlm_v2,
            sign_options=2,
            is_direct_tcp=is_direct_tcp,
        )
        conn.connect(self.host, port=self.port)
        return conn

    def _is_client_closed(self, client: SMBConnection) -> bool:
        try:
            socket_fileno = client.sock.fileno()
        except (AttributeError, OSError):
            return True

        return socket_fileno == -1

    def _close_client(self, client: SMBConnection) -> None:
        client.close()

    def _download_file(self, remote_file_path: RemotePath, local_file_path: LocalPath) -> None:
        with open(local_file_path, "wb") as local_file:
            self.client.retrieveFile(
                self.share,
                os.fspath(remote_file_path),
                local_file,
            )

    def _get_stat(self, path: RemotePath) -> RemotePathStat:
        info = self.client.getAttributes(self.share, os.fspath(path))

        if self.is_dir(os.fspath(path)):
            return RemotePathStat(st_mode=stat.S_IFDIR)

        return RemotePathStat(
            st_size=info.file_size,
            st_mtime=info.last_write_time,
            st_uid=info.filename,
        )

    def _remove_file(self, remote_file_path: RemotePath) -> None:
        self.client.deleteFiles(
            self.share,
            os.fspath(remote_file_path),
        )

    def _create_dir(self, path: RemotePath) -> None:
        path_obj = Path(path)
        for parent in reversed(path_obj.parents):
            # create dirs sequentially as .createDirectory(...) cannot create nested dirs
            try:
                self.client.getAttributes(self.share, os.fspath(parent))
            except OperationFailure:
                self.client.createDirectory(self.share, os.fspath(parent))

        self.client.createDirectory(self.share, os.fspath(path))

    def _upload_file(self, local_file_path: LocalPath, remote_file_path: RemotePath) -> None:
        with open(local_file_path, "rb") as file_obj:
            self.client.storeFile(
                self.share,
                os.fspath(remote_file_path),
                file_obj,
            )

    def _rename_file(self, source: RemotePath, target: RemotePath) -> None:
        self.client.rename(
            self.share,
            os.fspath(source),
            os.fspath(target),
        )

    def _remove_dir(self, path: RemotePath) -> None:
        files = self.client.listPath(self.share, os.fspath(path))

        for item in files:
            if item.filename not in {".", ".."}:  # skip current and parent directory entries
                full_path = path / item.filename
                if item.isDirectory:
                    # recursively delete subdirectory
                    self._remove_dir(full_path)
                else:
                    self.client.deleteFiles(self.share, os.fspath(full_path))

        self.client.deleteDirectory(self.share, os.fspath(path))

    def _read_text(self, path: RemotePath, encoding: str) -> str:
        return self._read_bytes(path).decode(encoding)

    def _read_bytes(self, path: RemotePath) -> bytes:
        file_obj = BytesIO()
        self.client.retrieveFile(
            self.share,
            os.fspath(path),
            file_obj,
        )
        file_obj.seek(0)
        return file_obj.read()

    def _write_text(self, path: RemotePath, content: str, encoding: str) -> None:
        self._write_bytes(path, bytes(content, encoding))

    def _write_bytes(self, path: RemotePath, content: bytes) -> None:
        file_obj = BytesIO(content)

        self.client.storeFile(
            self.share,
            os.fspath(path),
            file_obj,
        )

    def _is_dir(self, path: RemotePath) -> bool:
        return self.client.getAttributes(self.share, os.fspath(path)).isDirectory

    def _is_file(self, path: RemotePath) -> bool:
        return not self.client.getAttributes(self.share, os.fspath(path)).isDirectory

    @validator("port", pre=True, always=True)
    def _set_port_based_on_protocol(cls, port, values):
        if port is None:
            return 445 if values.get("protocol") == "SMB" else 139
        return port
