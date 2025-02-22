# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime
import io
import os
import stat
import textwrap
from logging import getLogger
from ssl import SSLContext
from typing import Optional, Union

from etl_entities.instance import Host

try:
    from pydantic.v1 import DirectoryPath, FilePath, SecretStr, root_validator
except (ImportError, AttributeError):
    from pydantic import DirectoryPath, FilePath, SecretStr, root_validator  # type: ignore[no-redef, assignment]

from typing_extensions import Literal

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.connection.file_connection.mixins.rename_dir_mixin import RenameDirMixin
from onetl.hooks import slot, support_hooks
from onetl.impl import LocalPath, RemotePath, RemotePathStat

try:
    from webdav3.client import Client
except (ImportError, NameError) as e:
    raise ImportError(
        textwrap.dedent(
            """
            Cannot import module "webdav3".

            Since onETL v0.7.0 you should install package as follows:
                pip install onetl[webdav]

            or
                pip install onetl[files]
            """,
        ).strip(),
    ) from e

log = getLogger(__name__)
DATA_MODIFIED_FORMAT = "%a, %d %b %Y %H:%M:%S %Z"


@support_hooks
class WebDAV(FileConnection, RenameDirMixin):
    """WebDAV file connection. |support_hooks|

    Based on `WebdavClient3 library <https://pypi.org/project/webdavclient3/>`_.

    .. warning::

        Since onETL v0.7.0 to use WebDAV connector you should install package as follows:

        .. code:: bash

            pip install onetl[webdav]

            # or
            pip install onetl[files]

        See :ref:`install-files` installation instruction for more details.

    .. versionadded:: 0.6.0

    Parameters
    ----------
    host : str
        Host of WebDAV source. For example: ``webdav.domain.com``

    user : str
        User, which have access to the file source. For example: ``someuser``

    password : str
        Password for file source connection

    ssl_verify : Union[Path, bool], optional
        SSL certificates used to verify the identity of requested hosts. Can be any of
            - ``True`` (uses default CA bundle),
            - a path to an SSL certificate file,
            - ``False`` (disable verification), or
            - a :obj:`ssl.SSLContext`

    protocol : str, default : ``https``
        Connection protocol. Allowed values: ``https`` or ``http``

    port : int, optional
        Connection port

    Examples
    --------

    Create and check WebDAV connection:

    .. code:: python

        from onetl.connection import WebDAV

        wd = WebDAV(
            host="webdav.domain.com",
            user="someuser",
            password="*****",
            protocol="https",
        ).check()
    """

    host: Host
    user: str
    password: SecretStr
    port: Optional[int] = None
    ssl_verify: Union[bool, FilePath, DirectoryPath, SSLContext] = True
    protocol: Literal["http", "https"] = "https"

    @root_validator
    def check_port(cls, values):
        if values["port"] is not None:
            return values

        if values["protocol"] == "https":
            values["port"] = 443
        else:
            values["port"] = 80

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
        options = {
            "webdav_hostname": f"{self.protocol}://{self.host}:{self.port}",
            "webdav_login": self.user,
            "webdav_password": self.password.get_secret_value(),
        }

        client = Client(options)
        client.verify = self.ssl_verify

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
            return RemotePathStat(st_mode=stat.S_IFDIR)

        return RemotePathStat(
            st_size=info["size"],
            st_mtime=datetime.datetime.strptime(info["modified"], DATA_MODIFIED_FORMAT).timestamp(),
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
            return RemotePathStat(st_mode=stat.S_IFDIR)

        return RemotePathStat(
            st_size=entry["size"],
            st_mtime=datetime.datetime.strptime(entry["modified"], DATA_MODIFIED_FORMAT).timestamp(),
            st_uid=entry["name"],
        )
