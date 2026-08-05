# SPDX-FileCopyrightText: 2021-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import contextlib
import os
import textwrap
import warnings
from logging import getLogger
from stat import S_ISDIR, S_ISREG

try:
    from pydantic.v1 import Field, FilePath, SecretStr, root_validator
except (ImportError, AttributeError):
    from pydantic import Field, FilePath, SecretStr, root_validator  # type: ignore[no-redef, assignment]

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.connection.file_connection.mixins.rename_dir_mixin import RenameDirMixin
from onetl.hooks import slot, support_hooks
from onetl.impl import GenericOptions, Host, LocalPath, RemotePath

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
                pip install "onetl[sftp]"

            or
                pip install "onetl[files]"
            """,
        ).strip(),
    ) from e

SSH_CONFIG_PATH = LocalPath("~/.ssh/config").expanduser().resolve()

log = getLogger(__name__)


class SFTPExtra(GenericOptions):
    """
    Extra options for SFTP connection.

    You can pass here any parameters supported by [paramiko.SSHClient](https://docs.paramiko.org/en/stable/api/client.html#paramiko.client.SSHClient).

    !!! success "Added in 0.16.0"

    Parameters
    ---------
    host_key_check : bool, optional
        Set to `True` to validate the SSH server's host key.
    timeout : float, optional
        Optional timeout (in seconds) for the TCP connect.
    banner_timeout : float, optional
        Optional timeout (in seconds) for the SSH banner.
    auth_timeout : float, optional
        Optional timeout (in seconds) for the SSH authentication.
    channel_timeout : float, optional
        Optional timeout (in seconds) for the SSH channel.
    compress : bool, optional
        Set to `True` to enable compression.

        !!! warning

            Not compatible with downloading/uploading compressed files.
    """

    host_key_check: bool = False
    timeout: float | None = None
    banner_timeout: float | None = None
    auth_timeout: float | None = None
    channel_timeout: float | None = None
    compress: bool = False

    class Config:
        extra = "allow"


@support_hooks
class SFTP(FileConnection, RenameDirMixin):
    """SFTP file connection. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](/hooks/)

    Based on [Paramiko library](https://pypi.org/project/paramiko/).

    !!! warning

        Since onETL v0.7.0 to use SFTP connector you should install package as follows:

        ```bash
        pip install "onetl[sftp]"

        # or
        pip install "onetl[files]"
        ```
        See [install-files][] installation instruction for more details.

    !!! success "Added in 0.1.0"

    Parameters
    ----------
    host : str
        Host of SFTP source. For example: `192.168.1.19`

    port : int, default: 22
        Port of SFTP source

    user : str
        User, which have access to the file source. For example: `someuser`

    password : str, optional
        Password for SFTP connection, optional.

    key_file : str, optional
        Path to private key file, optional.

    extra : SFTPExtra, optional
        Extra options for SFTP connection

    Examples
    --------

    === "Create SFTP connection with password"

        ```python
        from onetl.connection import SFTP

        sftp = SFTP(
            host="192.168.1.19",
            user="someuser",
            password="*****",
        ).check()
        ```

    === "Create SFTP connection with private key file"

        ```python
        from onetl.connection import SFTP

        sftp = SFTP(
            host="192.168.1.19",
            user="someuser",
            key_file="~/.ssh/id_rsa",
        ).check()
        ```

    === "Create SFTP connection with extra options"

        ```python
        from onetl.connection import SFTP

        sftp = SFTP(
            host="192.168.1.19",
            user="someuser",
            password="*****",
            extra=SFTP.Extra(host_key_check=True, timeout=60),
        ).check()
        ```
    """

    host: Host
    port: int = 22
    user: str | None = None
    password: SecretStr | None = None
    key_file: FilePath | None = None
    extra: SFTPExtra = Field(default_factory=SFTPExtra)

    Extra = SFTPExtra

    @root_validator(pre=True)
    def _extra_fallback(cls, values):
        extra_dict = cls.Extra.parse(values.get("extra")).dict(exclude_unset=True, by_alias=True)
        for key in ["timeout", "host_key_check", "compress"]:
            if key not in values:
                continue

            value = values.pop(key)
            warnings.warn(
                f"Option `{key}` is deprecated since v0.16.0 and will be removed in v1.0.0. "
                f"Use extra={cls.__name__}.Extra({key}={value!r}) instead",
                category=UserWarning,
                stacklevel=5,
            )
            extra_dict[key] = value
        values["extra"] = cls.Extra.parse(extra_dict)
        return values

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}"

    def __str__(self):
        return f"{self.__class__.__name__}[{self.host}:{self.port}]"

    @slot
    def path_exists(self, path: os.PathLike | str) -> bool:
        try:
            self.client.stat(os.fspath(path))
        except FileNotFoundError:
            return False
        else:
            return True

    def _get_client(self) -> SFTPClient:
        host_proxy, key_file = self._parse_user_ssh_config()

        client = SSHClient()
        client.load_system_host_keys()
        if not self.extra.host_key_check:
            # Default is RejectPolicy
            client.set_missing_host_key_policy(WarningPolicy())  # noqa: S507

        extra = self.extra.dict(by_alias=True, exclude={"host_key_check"})
        client.connect(
            hostname=self.host,
            port=self.port,
            username=self.user,
            password=self.password.get_secret_value() if self.password else None,
            key_filename=key_file,
            sock=host_proxy,
            **extra,
        )

        return client.open_sftp()

    def _is_client_closed(self, client: SFTPClient) -> bool:
        return not client.sock or client.sock.closed

    def _close_client(self, client: SFTPClient) -> None:
        client.close()

    def _parse_user_ssh_config(self) -> tuple[ProxyCommand | None, str | None]:
        host_proxy = None
        key_file = os.fspath(self.key_file) if self.key_file else None

        if not SSH_CONFIG_PATH.exists() or not SSH_CONFIG_PATH.is_file():
            return host_proxy, key_file

        try:
            ssh_conf = SSHConfig()
            ssh_conf.parse(SSH_CONFIG_PATH.read_text())
            host_info = ssh_conf.lookup(self.host) or {}

            proxycommand = host_info.get("proxycommand")
            if proxycommand:
                host_proxy = ProxyCommand(proxycommand)

            if not (self.password or key_file):
                identityfile = host_info.get("identityfile")
                if identityfile:
                    key_file = identityfile[0]
        except ConfigParseError:
            log.exception("Failed to parse SSH config")

        return host_proxy, key_file

    def _create_dir(self, path: RemotePath) -> None:
        try:
            self.client.stat(os.fspath(path))
        except OSError:
            for parent in reversed(path.parents):
                try:
                    self.client.stat(os.fspath(parent))
                except OSError:  # noqa: PERF203
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
        return entry.filename  # type: ignore[attr-defined]

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
