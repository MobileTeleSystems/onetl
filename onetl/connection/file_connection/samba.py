# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import os
import textwrap
from io import BytesIO
from logging import getLogger
from pathlib import Path
from typing import ClassVar, Literal

from pydantic import ConfigDict, Field, SecretStr, model_validator

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.hooks import slot, support_hooks
from onetl.impl import GenericOptions, Host, LocalPath, RemotePath, RemotePathStat

try:
    from smb.smb_structs import OperationFailure
    from smb.SMBConnection import SMBConnection
except (ImportError, NameError) as e:
    raise ImportError(
        textwrap.dedent(
            """
            Cannot import module "pysmb".

            You should install package as follows:
                pip install "onetl[samba]"

            or
                pip install "onetl[files]"
            """,
        ).strip(),
    ) from e


log = getLogger(__name__)


class SambaExtra(GenericOptions):
    """
    Extra options for Samba connection.

    You can pass here any parameters supported by [smb.SMBConnection.SMBConnection class](https://pysmb.readthedocs.io/en/latest/api/smb_SMBConnection.html).

    !!! success "Added in 0.16.0"

    Parameters
    ---------
    connect_timeout
        Timeout (in seconds) for establishing TCP connection.
    operation_timeout
        Timeout (in seconds) for the client operations.
    my_name
        Client name.
    sign_options
        Sign options.
    """

    connect_timeout: int = 60
    operation_timeout: int = 30
    my_name: str = "onetl"
    sign_options: int = SMBConnection.SIGN_WHEN_REQUIRED
    model_config = ConfigDict(extra="allow")


@support_hooks
class Samba(FileConnection):
    """Samba file connection. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

    Based on [pysmb library](https://pypi.org/project/pysmb/).

    !!! success "Added in 0.9.4"

    !!! warning

        To use Samba connector you should install package as follows:

        ```bash
        pip install "onetl[samba]"

        # or
        pip install "onetl[files]"
        ```
        See [DBR-onetl-install-files-file-connections][] installation instruction for more details.

    Parameters
    ----------
    host
        Host of Samba source. For example: `mydomain.com`.

    share
        The name of the share on the Samba server.

    protocol
        The protocol to use for the connection. Either `SMB` or `NetBIOS`.
        Affects the default port and the `is_direct_tcp` flag in `SMBConnection`.

    port
        Port of Samba source.

    domain
        Windows workgroup name. Empty strings means use `host` as domain name.

    auth_type
        The authentication type to use. Either `NTLMv2` (recommended) or `NTLMv1` (Windows XP).

    user
        User, which have access to the file source. Can be `None` for anonymous connection.

    password
        Password for file source connection. Can be `None` for anonymous connection.

    extra
        Extra options for Samba connection.

    Examples
    --------

    === "Create and check Samba connection"

        ```python
        from onetl.connection import Samba

        samba = Samba(
            host="mydomain.com",
            share="share_name",
            protocol="SMB",
            port=445,
            user="user",
            password="password",
        ).check()
        ```

    === "Create Samba connection with extra options"

        ```python
        from onetl.connection import Samba
        from smb.SMBConnection import SMBConnection

        samba = Samba(
            host="mydomain.com",
            share="share_name",
            protocol="SMB",
            port=445,
            user="user",
            password="password",
            extra=Samba.Extra(my_name="my_name", sign_options=SMBConnection.SIGN_NEVER),
        ).check()
        ```
    """

    host: Host
    share: str
    protocol: Literal["SMB", "NetBIOS"] = "SMB"
    port: int = 445
    domain: str = ""
    auth_type: Literal["NTLMv1", "NTLMv2"] = "NTLMv2"
    user: str | None = None
    password: SecretStr | None = None

    extra: SambaExtra = Field(default_factory=SambaExtra)

    Extra: ClassVar = SambaExtra

    @model_validator(mode="before")
    @classmethod
    def _set_port_based_on_protocol(cls, values):
        port = values.get("port")
        if port is not None:
            return values

        values["port"] = 445 if values.get("protocol", "SMB") == "SMB" else 139
        return values

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
                msg = "Failed to connect to the Samba server."
                raise ConnectionError(msg)  # noqa: TRY301
        except Exception as exc:
            log.exception("|%s| Connection is unavailable", self.__class__.__name__)
            msg = "Connection is unavailable"
            raise RuntimeError(msg) from exc

        return self

    @slot
    def path_exists(self, path: os.PathLike | str) -> bool:
        try:
            self.client.getAttributes(self.share, os.fspath(path))
        except OperationFailure:
            return False
        else:
            return True

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
            return RemotePathStat()

        return RemotePathStat(
            st_size=entry.file_size,
            st_mtime=entry.last_write_time,
            st_uid=entry.filename,
        )

    def _get_stat(self, path: RemotePath) -> RemotePathStat:
        info = self.client.getAttributes(self.share, os.fspath(path))

        if info.isDirectory:
            return RemotePathStat()

        return RemotePathStat(
            st_size=info.file_size,
            st_mtime=info.last_write_time,
            st_uid=info.filename,
        )

    def _get_client(self) -> SMBConnection:
        is_direct_tcp = self.protocol == "SMB"
        use_ntlm_v2 = self.auth_type == "NTLMv2"
        extra = self.extra.model_dump(by_alias=True, exclude={"operation_timeout", "connect_timeout"})
        conn = SMBConnection(
            username=self.user,
            password=self.password.get_secret_value() if self.password else None,
            remote_name=self.host,
            domain=self.domain,
            use_ntlm_v2=use_ntlm_v2,
            is_direct_tcp=is_direct_tcp,
            **extra,
        )
        auth_result = conn.connect(
            self.host,
            port=self.port,
            timeout=self.extra.connect_timeout,
        )
        if not auth_result:
            msg = "Failed to connect to the Samba server."
            raise ConnectionError(msg)
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
        with local_file_path.open("wb") as local_file:
            self.client.retrieveFile(
                self.share,
                os.fspath(remote_file_path),
                local_file,
                timeout=self.extra.operation_timeout,
            )

    def _create_dir(self, path: RemotePath) -> None:
        path_obj = Path(path)
        for parent in reversed(path_obj.parents):
            # create dirs sequentially as .createDirectory(...) cannot create nested dirs
            try:
                self.client.getAttributes(self.share, os.fspath(parent), timeout=self.extra.operation_timeout)
            except OperationFailure:  # noqa: PERF203
                self.client.createDirectory(self.share, os.fspath(parent), timeout=self.extra.operation_timeout)

        self.client.createDirectory(self.share, os.fspath(path), timeout=self.extra.operation_timeout)

    def _upload_file(self, local_file_path: LocalPath, remote_file_path: RemotePath) -> None:
        with local_file_path.open("rb") as file_obj:
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
            timeout=self.extra.operation_timeout,
        )

    def _remove_file(self, remote_file_path: RemotePath) -> None:
        self.client.deleteFiles(
            self.share,
            os.fspath(remote_file_path),
            timeout=self.extra.operation_timeout,
        )

    def _remove_dir(self, path: RemotePath) -> None:
        self.client.deleteDirectory(
            self.share,
            os.fspath(path),
            timeout=self.extra.operation_timeout,
        )

    def _remove_dir_recursive(self, root: RemotePath) -> None:
        self.client.deleteFiles(
            self.share,
            os.fspath(root).rstrip("/") + "/*",
            delete_matching_folders=True,
            timeout=self.extra.operation_timeout,
        )
        self.client.deleteDirectory(
            self.share,
            os.fspath(root),
            timeout=self.extra.operation_timeout,
        )

    def _read_text(self, path: RemotePath, encoding: str) -> str:
        return self._read_bytes(path).decode(encoding)

    def _read_bytes(self, path: RemotePath) -> bytes:
        file_obj = BytesIO()
        self.client.retrieveFile(
            self.share,
            os.fspath(path),
            file_obj,
            timeout=self.extra.operation_timeout,
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
            timeout=self.extra.operation_timeout,
        )

    def _is_dir(self, path: RemotePath) -> bool:
        attributes = self.client.getAttributes(self.share, os.fspath(path), timeout=self.extra.operation_timeout)
        return attributes.isDirectory

    def _is_file(self, path: RemotePath) -> bool:
        return not self._is_dir(path)
