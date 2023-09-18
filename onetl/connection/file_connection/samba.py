#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import os
import stat
import textwrap
from io import BytesIO
from logging import getLogger
from typing import Optional

from etl_entities.instance import Host
from pydantic import SecretStr, validator

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.hooks import slot, support_hooks
from onetl.impl import LocalPath, RemotePath, RemotePathStat

try:
    from smb.base import NotConnectedError
    from smb.smb_structs import OperationFailure
    from smb.SMBConnection import SMBConnection
except (ImportError, NameError) as e:
    raise ImportError(
        textwrap.dedent(
            """
            Cannot import module "pysmb".

            Since onETL v0.7.0 you should install package as follows:
                pip install onetl[samba]

            or
                pip install onetl[files]
            """,
        ).strip(),
    ) from e


log = getLogger(__name__)


@support_hooks
class Samba(FileConnection):
    """Samba file connection.

    Based on `pysmb library <https://pypi.org/project/pysmb/>`_.

    Parameters
    ----------
    host : str
        Host of Samba source. For example: ``msk.mts.ru``.

    share : str
        The name of the share on the Samba server.

    protocol : str, default: ``SMB``
        The protocol to use for the connection. Either ``SMB`` or ``NetBIOS``.
        Affects the default port and the `is_direct_tcp` flag in `SMBConnection`.

    port : int, default: 445
        Port of Samba source.

    domain : str, default: ``
        Domain name for the Samba connection.

    auth_type : str, default: ``NTLMv2``
        The authentication type to use. Either ``NTLMv2`` or ``NTLMv1``.
        Affects the `use_ntlm_v2` flag in `SMBConnection`.

    user : str, default: None
        User, which have access to the file source. Can be `None` for anonymous connection.

    password : str, default: None
        Password for file source connection. Can be `None` for anonymous connection.

    timeout : int, default: ``10``
        How long to wait for the server to send data before giving up.
    """

    host: Host
    share: str
    protocol: str = "SMB"
    port: Optional[int] = None
    domain: Optional[str] = ""
    auth_type: str = "NTLMv2"
    user: Optional[str] = None
    password: Optional[SecretStr] = None
    timeout: int = 10

    @property
    def instance_url(self) -> str:
        return f"smb://{self.host}:{self.port}"

    @validator("port", pre=True, always=True)
    def set_port_based_on_protocol(cls, port, values):
        if port is None:
            return 445 if values.get("protocol") == "SMB" else 139
        return port

    @slot
    def check(self):
        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()
        try:
            available_shares = {share.name for share in self.client.listShares()}
            if self.share in available_shares:
                log.info("|%s| Connection is available.", self.__class__.__name__)
            else:
                raise ConnectionError("Failed to connect to the Samba server.")
        except (RuntimeError, ValueError):
            # left validation errors intact
            log.exception("|%s| Connection is unavailable", self.__class__.__name__)
            raise
        except Exception as exc:
            log.exception("|%s| Connection is unavailable", self.__class__.__name__)
            raise RuntimeError("Connection is unavailable") from exc

        return self

    @slot
    def path_exists(self, path: os.PathLike | str) -> bool:
        try:
            self.client.getAttributes(self.share, str(path))
            return True
        except OperationFailure:
            return False

    def _scan_entries(self, path: RemotePath) -> list:
        if self._is_dir(path):
            return [
                entry
                for entry in self.client.listPath(
                    self.share,
                    str(path),
                )
                if entry.filename not in {".", ".."}  # Filter out '.' and '..'
            ]  # pysmb replaces '/', not works with <RemotePath> type
        return [self.client.getAttributes(self.share, (os.fspath(path)))]

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
            my_name="optional_client_name",
            remote_name=self.host,
            domain=self.domain,
            use_ntlm_v2=use_ntlm_v2,
            sign_options=2,
            is_direct_tcp=is_direct_tcp,
        )
        conn.connect(self.host, port=self.port)
        return conn  # noqa: WPS331

    def _is_client_closed(self, client: SMBConnection) -> bool:
        try:
            client.listShares()
        except NotConnectedError:
            return True
        return False

    def _close_client(self, client: SMBConnection) -> None:
        self.client.close()

    def _download_file(self, remote_file_path: RemotePath, local_file_path: LocalPath) -> None:
        with open(local_file_path, "wb") as local_file:
            self.client.retrieveFile(
                self.share,
                str(remote_file_path),
                local_file,
                show_progress=True,
            )  # pysmb replaces '/', not works with <RemotePath> type

    def _get_stat(self, path: RemotePath) -> RemotePathStat:
        info = self.client.getAttributes(self.share, (os.fspath(path)))

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
            str(remote_file_path),
        )  # pysmb replaces '/', not works with <RemotePath> type

    def _create_dir(self, path: RemotePath) -> None:
        path_parts = str(path).strip("/").split("/")
        current_path_parts = []
        for part in path_parts:  # create dirs sequentially as .createDirectory(...) cannot create nested dirs
            current_path_parts.append(part)
            current_path = "/".join(current_path_parts)
            try:
                self.client.createDirectory(self.share, current_path)
            except OperationFailure:
                pass

    def _upload_file(self, local_file_path: LocalPath, remote_file_path: RemotePath) -> None:
        with open(local_file_path, "rb") as file_obj:
            self.client.storeFile(
                self.share,
                str(remote_file_path),
                file_obj,
            )  # pysmb replaces '/', not works with <RemotePath> type

    def _rename_file(self, source: RemotePath, target: RemotePath) -> None:
        self.client.rename(
            self.share,
            str(source),
            str(target),
        )  # pysmb replaces '/', not works with <RemotePath> type

    def _remove_dir(self, path: RemotePath) -> None:
        files = self.client.listPath(self.share, str(path))

        for f in files:
            if f.filename not in {".", ".."}:  # skip current and parent directory entries
                full_path = f"{path}/{f.filename}"
                if f.isDirectory:
                    # recursively delete subdirectory
                    self._remove_dir(full_path)
                else:
                    self.client.deleteFiles(self.share, full_path)

        self.client.deleteDirectory(self.share, str(path))

    def _read_text(self, path: RemotePath, encoding: str, **kwargs) -> str:
        file_obj = BytesIO()
        self.client.retrieveFile(
            self.share,
            str(path),
            file_obj,
        )  # pysmb replaces '/', not works with <RemotePath> type
        file_obj.seek(0)
        return file_obj.read().decode(encoding)

    def _read_bytes(self, path: RemotePath) -> bytes:
        file_obj = BytesIO()
        self.client.retrieveFile(
            self.share,
            str(path),
            file_obj,
        )  # pysmb replaces '/', not works with <RemotePath> type
        file_obj.seek(0)
        return file_obj.read()

    def _write_text(self, path: RemotePath, content: str, encoding: str) -> None:
        file_obj = BytesIO(content.encode(encoding))

        self.client.storeFile(
            self.share,
            str(path),
            file_obj,
        )  # pysmb replaces '/', not works with <RemotePath> type

    def _write_bytes(self, path: RemotePath, content: bytes) -> None:
        file_obj = BytesIO(content)

        self.client.storeFile(
            self.share,
            str(path),
            file_obj,
        )  # pysmb replaces '/', not works with <RemotePath> type

    def _is_dir(self, path: RemotePath) -> bool:
        return self.client.getAttributes(self.share, (os.fspath(path))).isDirectory

    def _is_file(self, path: RemotePath) -> bool:
        return not self.client.getAttributes(self.share, (os.fspath(path))).isDirectory
