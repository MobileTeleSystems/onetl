# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import datetime
import io
import os
import textwrap
import warnings
from logging import getLogger
from pathlib import Path
from typing import Any, ClassVar, Literal

from pydantic import (
    ConfigDict,
    DirectoryPath,
    Field,
    FilePath,
    SecretStr,
    model_validator,
)

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.connection.file_connection.mixins.rename_dir_mixin import RenameDirMixin
from onetl.hooks import slot, support_hooks
from onetl.impl import GenericOptions, Host, LocalPath, RemotePath, RemotePathStat

try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from urllib3.util.timeout import Timeout
    from webdav3.client import Client

except (ImportError, NameError) as e:
    raise ImportError(
        textwrap.dedent(
            """
            Cannot import module "webdav3".

            Since onETL v0.7.0 you should install package as follows:
                pip install "onetl[webdav]"

            or
                pip install "onetl[files]"
            """,
        ).strip(),
    ) from e

log = getLogger(__name__)
DATA_MODIFIED_FORMAT = "%a, %d %b %Y %H:%M:%S %Z"


class WebDAVExtra(GenericOptions):
    """
    Extra options for WebDAV connection.

    You can pass here any parameters supported by [webdav3.client.Client](https://github.com/ezhov-evgeny/webdav-client-python-3#webdav-api),
    **without** `webdav_` prefix.

    !!! success "Added in 0.16.0"

    Parameters
    ---------
    timeout
        Timeout for requests,  see [urllib3 documentation](https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html#urllib3.util.Timeout).
    retry
        Retry for requests, see [urllib3 documentation](https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html#urllib3.util.Retry).
    ssl_verify
        One of:

        - a path to a file with SSL certificate.
        - a path to a directory with SSL certificates.
        - `True` to use default SSL certificates.
        - `False` to disable SSL certificate verification.
    """

    timeout: Timeout = Timeout(connect=10, read=60)
    retry: Retry = Retry.DEFAULT

    ssl_verify: FilePath | DirectoryPath | bool = Field(default=True, validate_default=True)

    @model_validator(mode="before")
    @classmethod
    def _ssl_verify_default_value(cls, values):
        value = values.get("ssl_verify", True)
        if value is True:
            # Try to use default SSL certificates
            for env_var in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE", "SSL_CERT_DIR"):
                value = os.environ.get(env_var)
                if not value:
                    continue
                values["ssl_verify"] = value
                return values

            import certifi

            values["ssl_verify"] = certifi.where()
            return values

        return values

    model_config = ConfigDict(extra="allow")


@support_hooks
class WebDAV(FileConnection, RenameDirMixin):
    """WebDAV file connection. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

    Based on [WebdavClient3 library](https://pypi.org/project/webdavclient3/).

    !!! warning

        Since onETL v0.7.0 to use WebDAV connector you should install package as follows:

        ```bash
        pip install "onetl[webdav]"

        # or
        pip install "onetl[files]"
        ```
        See [DBR-onetl-install-files-file-connections][] installation instruction for more details.

    !!! success "Added in 0.6.0"

    Parameters
    ----------
    host
        Host of WebDAV source. For example: `webdav.domain.com`

    user
        User, which have access to the file source. For example: `someuser`

    password
        Password for file source connection

    protocol
        Connection protocol. Allowed values: `https` or `http`

    port
        Connection port

    extra
        Extra options passed to WebDAV client

    Examples
    --------

    === "Create and check WebDAV connection"

        ```python
        from onetl.connection import WebDAV

        wd = WebDAV(
            host="webdav.domain.com",
            user="someuser",
            password="*****",
            protocol="https",
        ).check()
        ```

    === "Create and check WebDAV connection with extra options"

        ```python
        from onetl.connection import WebDAV
        from urllib3.util.timeout import Timeout
        from urllib3.util.retry import Retry

        wd = WebDAV(
            host="webdav.domain.com",
            user="someuser",
            password="*****",
            extra=WebDAV.Extra(
                ssl_verify=True,
                timeout=Timeout(connect=10, read=60),
                retry=Retry(
                    total=3,
                    backoff_factor=0.2,
                    # retry on missing files (404 status code)
                    status_forcelist=[404, 429, 500, 502, 503, 504],
                ),
                disable_check=True,
            )
        ).check()
        ```
    """

    host: Host
    user: str
    password: SecretStr
    protocol: Literal["http", "https"] = "https"
    port: int = 443
    extra: WebDAVExtra = Field(default_factory=WebDAVExtra)

    Extra: ClassVar = WebDAVExtra

    @model_validator(mode="before")
    @classmethod
    def _set_port_based_on_protocol(cls, values):
        port = values.get("port")
        if port is not None:
            return values

        values["port"] = 443 if values.get("protocol", "https") == "https" else 80
        return values

    @model_validator(mode="before")
    @classmethod
    def _ssl_verify_fallback(cls, values):
        ssl_verify = values.pop("ssl_verify", None)
        if ssl_verify is None:
            return values

        warnings.warn(
            "Option `ssl_verify` is deprecated since v0.16.0 and will be removed in v1.0.0. "
            f"Use extra={cls.__name__}.Extra(ssl_verify={ssl_verify!r}) instead",
            category=UserWarning,
            stacklevel=3,
        )
        values["extra"] = cls.Extra.parse(
            cls.Extra.parse(values.get("extra")).model_dump(exclude_unset=True, by_alias=True)
            | {"ssl_verify": ssl_verify}
        )
        return values

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}"

    def __str__(self):
        return f"{self.__class__.__name__}[{self.host}:{self.port}]"

    @slot
    def path_exists(self, path: os.PathLike | str) -> bool:
        return self.client.check(os.fspath(path))

    def _get_client(self) -> Client:
        options: dict[str, Any] = {
            "webdav_hostname": f"{self.protocol}://{self.host}:{self.port}",
            "webdav_login": self.user,
            "webdav_password": self.password.get_secret_value(),
            "webdav_timeout": (self.extra.timeout.connect_timeout, self.extra.timeout.read_timeout),
        }

        extra = self.extra.model_dump(by_alias=True, exclude={"timeout", "retry", "ssl_verify"})
        options.update({"webdav_" + k: v for k, v in extra.items()})

        client = Client(options)
        client.session.mount(f"{self.protocol}://", HTTPAdapter(max_retries=self.extra.retry))
        client.verify = self.extra.ssl_verify
        if isinstance(client.verify, Path):
            client.verify = os.fspath(client.verify)
        client.chunk_size = client.webdav.chunk_size
        return client

    def _is_client_closed(self, client: Client):
        return False

    def _close_client(self, client: Client) -> None:  # NOSONAR
        pass

    def _download_file(self, remote_file_path: RemotePath, local_file_path: LocalPath) -> None:
        self.client.download_sync(
            remote_path=os.fspath(remote_file_path),
            local_path=os.fspath(local_file_path),
        )

    def _get_stat(self, path: RemotePath) -> RemotePathStat:
        info = self.client.info(os.fspath(path))

        if self.client.is_dir(os.fspath(path)):
            return RemotePathStat()

        mtime = datetime.datetime.strptime(info["modified"], DATA_MODIFIED_FORMAT)  # noqa: DTZ007
        return RemotePathStat(
            st_size=info["size"],
            st_mtime=mtime.timestamp(),
            st_uid=info["name"],
        )

    def _remove_file(self, remote_file_path: RemotePath) -> None:
        self.client.clean(os.fspath(remote_file_path))

    def _create_dir(self, path: RemotePath) -> None:
        for directory in reversed(path.parents):  # from root to nested directory
            if not self.path_exists(directory):
                self.client.mkdir(os.fspath(directory))
        self.client.mkdir(os.fspath(path))

    def _upload_file(self, local_file_path: LocalPath, remote_file_path: RemotePath) -> None:
        self.client.upload_sync(
            local_path=os.fspath(local_file_path),
            remote_path=os.fspath(remote_file_path),
        )

    def _rename_file(self, source: RemotePath, target: RemotePath) -> None:
        res = self.client.resource(os.fspath(source))
        res.move(os.fspath(target))

    _rename_dir = _rename_file

    def _scan_entries(self, path: RemotePath) -> list[dict]:
        return self.client.list(os.fspath(path), get_info=True)

    def _remove_dir(self, path: RemotePath) -> None:
        self.client.clean(os.fspath(path))

    def _read_text(self, path: RemotePath, encoding: str) -> str:
        res = self.client.resource(os.fspath(path))
        stream = io.BytesIO()
        res.write_to(stream)

        return stream.getvalue().decode(encoding)

    def _read_bytes(self, path: RemotePath) -> bytes:
        res = self.client.resource(os.fspath(path))
        stream = io.BytesIO()
        res.write_to(stream)

        return stream.getvalue()

    def _write_text(self, path: RemotePath, content: str, encoding: str) -> None:
        res = self.client.resource(os.fspath(path))
        content_bytes = content.encode(encoding)
        stream = io.BytesIO(content_bytes)
        res.read_from(buff=stream)

    def _write_bytes(self, path: RemotePath, content: bytes) -> None:
        res = self.client.resource(os.fspath(path))
        stream = io.BytesIO(content)
        res.read_from(buff=stream)

    def _is_dir(self, path: RemotePath) -> bool:
        return self.client.is_dir(os.fspath(path))

    def _is_file(self, path: RemotePath) -> bool:
        return not self.client.is_dir(os.fspath(path))

    def _extract_name_from_entry(self, entry: dict) -> str:
        return RemotePath(entry["path"]).name

    def _is_dir_entry(self, top: RemotePath, entry: dict) -> bool:
        return entry["isdir"]

    def _is_file_entry(self, top: RemotePath, entry: dict) -> bool:
        return not entry["isdir"]

    def _extract_stat_from_entry(self, top: RemotePath, entry: dict) -> RemotePathStat:
        if entry["isdir"]:
            return RemotePathStat()

        mtime = datetime.datetime.strptime(entry["modified"], DATA_MODIFIED_FORMAT)  # noqa: DTZ007
        return RemotePathStat(
            st_size=entry["size"],
            st_mtime=mtime.timestamp(),
            st_uid=entry["name"],
        )
