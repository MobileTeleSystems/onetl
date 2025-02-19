# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import io
import os
import textwrap
from contextlib import suppress
from logging import getLogger
from typing import Optional

from onetl.hooks import slot, support_hooks

try:
    from minio import Minio, commonconfig
    from minio.datatypes import Object
except (ImportError, NameError) as e:
    raise ImportError(
        textwrap.dedent(
            """
            Cannot import module "minio".

            Since onETL v0.7.0 you should install package as follows:
                pip install onetl[s3]

            or
                pip install onetl[files]
            """,
        ).strip(),
    ) from e

from etl_entities.instance import Host

try:
    from pydantic.v1 import SecretStr, root_validator
except (ImportError, AttributeError):
    from pydantic import SecretStr, root_validator  # type: ignore[no-redef, assignment]

from typing_extensions import Literal

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.impl import LocalPath, RemoteDirectory, RemotePath, RemotePathStat

log = getLogger(__name__)


@support_hooks
class S3(FileConnection):
    """S3 file connection. |support_hooks|

    Based on `minio-py client <https://pypi.org/project/minio/>`_.

    .. warning::

        Since onETL v0.7.0 to use S3 connector you should install package as follows:

        .. code:: bash

            pip install onetl[s3]

            # or
            pip install onetl[files]

        See :ref:`install-files` installation instruction for more details.

    .. versionadded:: 0.5.1

    Parameters
    ----------
    host : str
        Host of S3 source. For example: ``s3.domain.com``

    port : int, optional
        Port of S3 source

    bucket : str
        Bucket name in the S3 file source

    access_key : str
        Access key (aka user ID) of an account in the S3 service

    secret_key : str
        Secret key (aka password) of an account in the S3 service

    protocol : str, default : ``https``
        Connection protocol. Allowed values: ``https`` or ``http``

        .. versionchanged:: 0.6.0
            Renamed ``secure: bool`` to ``protocol: Literal["https", "http"]``

    session_token : str, optional
        Session token of your account in S3 service

    region : str, optional
        Region name of bucket in S3 service

    Examples
    --------

    Create and check S3 connection:

    .. code:: python

        from onetl.connection import S3

        s3 = S3(
            host="s3.domain.com",
            protocol="http",
            bucket="my-bucket",
            access_key="ACCESS_KEY",
            secret_key="SECRET_KEY",
        ).check()

    """

    host: Host
    port: Optional[int] = None
    bucket: str
    access_key: str
    secret_key: SecretStr
    protocol: Literal["http", "https"] = "https"
    session_token: Optional[SecretStr] = None
    region: Optional[str] = None

    @root_validator
    def validate_port(cls, values):
        if values["port"] is not None:
            return values

        values["port"] = 443 if values["protocol"] == "https" else 80
        return values

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}/{self.bucket}"

    def __str__(self):
        return f"{self.__class__.__name__}[{self.host}:{self.port}/{self.bucket}]"

    @slot
    def create_dir(self, path: os.PathLike | str) -> RemoteDirectory:
        # the method is overridden because S3 does not create a directory
        # and the method must return the created directory

        log.debug("|%s| Creating directory '%s'", self.__class__.__name__, path)
        remote_directory = RemotePath(path)

        if self.path_exists(remote_directory):
            return self.resolve_dir(remote_directory)

        self._create_dir(remote_directory)
        log.info("|%s| Successfully created directory '%s'", self.__class__.__name__, remote_directory)
        return RemoteDirectory(path=remote_directory, stats=RemotePathStat())

    @slot
    def path_exists(self, path: os.PathLike | str) -> bool:
        remote_path = RemotePath(os.fspath(path))
        if self._is_root(remote_path):
            return True

        remote_path_str = self._delete_absolute_path_slash(remote_path)
        for component in self.client.list_objects(self.bucket, prefix=remote_path_str):
            component_path = RemotePath(component.object_name)
            component_path_str = self._delete_absolute_path_slash(component_path)
            if component_path_str == remote_path_str:
                return True

        return False

    def _get_client(self) -> Minio:
        return Minio(
            endpoint=f"{self.host}:{self.port}",
            access_key=self.access_key,
            secret_key=self.secret_key.get_secret_value(),
            secure=self.protocol == "https",
            session_token=self.session_token.get_secret_value() if self.session_token else None,
            region=self.region,
        )

    def _is_client_closed(self, client: Minio):
        return False

    def _close_client(self, client: Minio) -> None:  # NOSONAR
        pass

    @staticmethod
    def _is_root(path: RemotePath) -> bool:
        return path.name == ""

    @classmethod
    def _delete_absolute_path_slash(cls, path: RemotePath) -> str:
        if cls._is_root(path):
            return ""

        if path.is_absolute():
            path = path.relative_to("/")

        return os.fspath(path)

    def _download_file(self, remote_file_path: RemotePath, local_file_path: LocalPath) -> None:
        path_str = self._delete_absolute_path_slash(remote_file_path)
        self.client.fget_object(self.bucket, path_str, os.fspath(local_file_path))

    def _get_stat(self, path: RemotePath) -> RemotePathStat:
        path_str = self._delete_absolute_path_slash(path)

        with suppress(Exception):
            # for some reason, client.stat_object returns less precise st_mtime than client.list_objects
            objects = self.client.list_objects(self.bucket, prefix=path_str)
            for obj in objects:
                if obj.object_name == path_str:
                    return self._extract_stat_from_entry(path.parent, obj)

        return RemotePathStat()

    def _remove_file(self, remote_file_path: RemotePath) -> None:
        path_str = self._delete_absolute_path_slash(remote_file_path)
        self.client.remove_object(self.bucket, path_str)

    def _create_dir(self, path: RemotePath) -> None:
        # in s3 dirs do not exist
        pass

    def _upload_file(self, local_file_path: LocalPath, remote_file_path: RemotePath) -> None:
        path_str = self._delete_absolute_path_slash(remote_file_path)
        self.client.fput_object(self.bucket, path_str, os.fspath(local_file_path))

    def _rename_file(self, source: RemotePath, target: RemotePath) -> None:
        source_str = self._delete_absolute_path_slash(source)
        target_str = self._delete_absolute_path_slash(target)
        self.client.copy_object(
            bucket_name=self.bucket,
            object_name=target_str,
            source=commonconfig.CopySource(self.bucket, source_str),
        )

        self._remove_file(source)

    def _scan_entries(self, path: RemotePath) -> list[Object]:
        if self._is_root(path):
            return self.client.list_objects(self.bucket)

        path_str = self._delete_absolute_path_slash(path)
        return self.client.list_objects(self.bucket, prefix=path_str + "/")

    def _extract_name_from_entry(self, entry: Object) -> str:
        return RemotePath(entry.object_name).name

    def _is_dir_entry(self, top: RemotePath, entry: Object) -> bool:
        return entry.is_dir

    def _is_file_entry(self, top: RemotePath, entry: Object) -> bool:
        return not entry.is_dir

    def _extract_stat_from_entry(self, top: RemotePath, entry: Object) -> RemotePathStat:
        return RemotePathStat(
            st_size=entry.size if entry.size else 0,
            st_mtime=entry.last_modified.timestamp() if entry.last_modified else None,
            st_uid=entry.owner_name or entry.owner_id,
        )

    def _remove_dir(self, path: RemotePath) -> None:
        # Empty. S3 does not have directories.
        pass

    def _read_text(self, path: RemotePath, encoding: str, **kwargs) -> str:
        path_str = self._delete_absolute_path_slash(path)
        file_handler = self.client.get_object(
            self.bucket,
            path_str,
            **kwargs,
        )
        return file_handler.read().decode(encoding)

    def _read_bytes(self, path: RemotePath, **kwargs) -> bytes:
        path_str = self._delete_absolute_path_slash(path)
        file_handler = self.client.get_object(self.bucket, path_str, **kwargs)
        return file_handler.read()

    def _write_text(self, path: RemotePath, content: str, encoding: str, **kwargs) -> None:
        content_bytes = content.encode(encoding)
        stream = io.BytesIO(content_bytes)
        self.client.put_object(
            self.bucket,
            data=stream,
            object_name=self._delete_absolute_path_slash(path),
            length=len(content_bytes),
            **kwargs,
        )

    def _write_bytes(self, path: RemotePath, content: bytes, **kwargs) -> None:
        stream = io.BytesIO(content)
        self.client.put_object(
            self.bucket,
            data=stream,
            object_name=self._delete_absolute_path_slash(path),
            length=len(content),
            **kwargs,
        )

    def _is_dir(self, path: RemotePath) -> bool:
        if self._is_root(path):
            return True

        path_str = self._delete_absolute_path_slash(path)
        return bool(list(self.client.list_objects(self.bucket, prefix=path_str + "/")))

    def _is_file(self, path: RemotePath) -> bool:
        path_str = self._delete_absolute_path_slash(path)

        try:
            self.client.stat_object(self.bucket, path_str)
            return True
        except Exception:  # noqa: B001, E722
            return False
