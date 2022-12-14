#  Copyright 2022 MTS (Mobile Telesystems)
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

import io
import os
from logging import getLogger
from typing import Any, Optional, Union

from minio import Minio, commonconfig
from minio.datatypes import Object
from pydantic import SecretStr, root_validator
from typing_extensions import Literal

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.impl import LocalPath, RemoteDirectory, RemotePath, RemotePathStat

log = getLogger(__name__)


class S3(FileConnection):
    """Class for S3 file connection.

    Parameters
    ----------
    host : str
        Host of S3 source. For example: ``s3.domain.com``

    port : int, optional
        Port of S3 source

    access_key : str
        Access key (aka user ID) of an account in the S3 service

    secret_key : str
        Secret key (aka password) of an account in the S3 service

    bucket : str
        Bucket name in the S3 file source

    protocol : str, default : ``https``
        Connection protocol. Allowed values: ``https`` or ``http``

    session_token : str, optional
        Session token of your account in S3 service

    region : str, optional
        Region name of bucket in S3 service

    Examples
    --------

    S3 file connection initialization

    .. code:: python

        from onetl.connection import S3

        s3 = S3(
            host="s3.domain.com",
            access_key="ACCESS_KEY",
            secret_key="SECRET_KEY",
            protocol="http",
        )

    """

    host: str
    port: Optional[int] = None
    access_key: str
    secret_key: SecretStr
    bucket: str
    protocol: Union[Literal["http"], Literal["https"]] = "https"
    session_token: Optional[str] = None
    region: Optional[str] = None

    @root_validator
    def check_port(cls, values):  # noqa: N805
        if values["port"] is not None:
            return values
        values["port"] = 443 if values["protocol"] == "https" else 80

        return values

    def mkdir(self, path: os.PathLike | str) -> RemoteDirectory:
        # the method is overridden because S3 does not create a directory
        # and the method must return the created directory

        log.debug(f"|{self.__class__.__name__}| Creating directory '{path}'")
        remote_directory = RemotePath(path)

        if self.path_exists(remote_directory):
            return self.get_directory(remote_directory)

        self._mkdir(remote_directory)
        log.info(f"|{self.__class__.__name__}| Successfully created directory '{remote_directory}'")
        return RemoteDirectory(path=remote_directory, stats=RemotePathStat())

    def path_exists(self, path: os.PathLike | str) -> bool:
        remote_path = RemotePath(os.fspath(path))

        remote_path = self._delete_absolute_path_slash(remote_path)

        if remote_path == RemotePath("."):
            return True

        for component in self.client.list_objects(self.bucket, prefix=os.fspath(remote_path)):
            if RemotePath(component.object_name) == remote_path:
                return True

        return False

    def _get_client(self) -> Any:
        return Minio(
            endpoint=f"{self.host}:{self.port}",
            access_key=self.access_key,
            secret_key=self.secret_key.get_secret_value(),
            secure=self.protocol == "https",
            session_token=self.session_token,
            region=self.region,
        )

    def _is_client_closed(self) -> bool:
        pass  # noqa: WPS420

    def _close_client(self) -> None:
        pass  # noqa: WPS420

    def _download_file(self, remote_file_path: RemotePath, local_file_path: LocalPath) -> None:
        remote_file_path = self._delete_absolute_path_slash(remote_file_path)

        self.client.fget_object(self.bucket, os.fspath(remote_file_path), os.fspath(local_file_path))

    def _get_stat(self, path: RemotePath) -> RemotePathStat:
        path = self._delete_absolute_path_slash(path)

        try:
            stat = self.client.stat_object(self.bucket, os.fspath(path))
            return RemotePathStat(
                st_size=stat.size or 0,
                st_mtime=stat.last_modified.timestamp() if stat.last_modified else None,
                st_uid=stat.owner_name or stat.owner_id,
            )

        except Exception:  # noqa: B001,E722
            return RemotePathStat()

    def _remove_file(self, remote_file_path: RemotePath) -> None:
        self.client.remove_object(self.bucket, os.fspath(remote_file_path))

    def _mkdir(self, path: RemotePath) -> None:
        # in s3 dirs do not exist
        pass  # noqa: WPS420

    @staticmethod
    def _delete_absolute_path_slash(path: RemotePath) -> RemotePath:
        if path.is_absolute():
            return path.relative_to("/")

        return path

    def _upload_file(self, local_file_path: LocalPath, remote_file_path: RemotePath) -> None:
        remote_file_path = self._delete_absolute_path_slash(remote_file_path)
        self.client.fput_object(self.bucket, os.fspath(remote_file_path), os.fspath(local_file_path))

    def _rename(self, source: RemotePath, target: RemotePath) -> None:
        source_str = os.fspath(self._delete_absolute_path_slash(source))
        target_str = os.fspath(self._delete_absolute_path_slash(target))
        self.client.copy_object(
            bucket_name=self.bucket,
            object_name=target_str,
            source=commonconfig.CopySource(self.bucket, source_str),
        )

        self._remove_file(source_str)

    def _scan_entries(self, path: RemotePath) -> list[Object]:
        return self.client.list_objects(self.bucket, prefix=os.fspath(path) + "/")

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

    def _rmdir(self, path: RemotePath) -> None:
        # Empty. S3 does not have directories.
        pass  # noqa: WPS420

    def _read_text(self, path: RemotePath, encoding: str, **kwargs) -> str:
        path = self._delete_absolute_path_slash(path)

        file_handler = self.client.get_object(
            self.bucket,
            os.fspath(path),
            **kwargs,
        )
        return file_handler.read().decode(encoding)

    def _read_bytes(self, path: RemotePath, **kwargs) -> bytes:
        path = self._delete_absolute_path_slash(path)

        file_handler = self.client.get_object(self.bucket, os.fspath(path), **kwargs)

        return file_handler.read()

    def _write_text(self, path: RemotePath, content: str, encoding: str, **kwargs) -> None:
        content_bytes = content.encode(encoding)
        stream = io.BytesIO(content_bytes)
        self.client.put_object(
            self.bucket,
            data=stream,
            object_name=os.fspath(self._delete_absolute_path_slash(path)),
            length=len(content_bytes),
            **kwargs,
        )

    def _write_bytes(self, path: RemotePath, content: bytes, **kwargs) -> None:
        stream = io.BytesIO(content)
        self.client.put_object(
            self.bucket,
            data=stream,
            object_name=os.fspath(path),
            length=len(content),
            **kwargs,
        )

    def _is_dir(self, path: RemotePath) -> bool:
        if RemotePath(path).name == "":
            # For the root directory
            return True

        path = self._delete_absolute_path_slash(path)

        return bool(list(self.client.list_objects(self.bucket, prefix=os.fspath(path) + "/")))

    def _is_file(self, path: RemotePath) -> bool:
        path = self._delete_absolute_path_slash(path)

        try:
            self.client.stat_object(self.bucket, os.fspath(path))
            return True
        except Exception:  # noqa: B001, E722
            return False
