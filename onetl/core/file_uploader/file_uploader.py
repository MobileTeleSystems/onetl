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
from logging import getLogger
from typing import Iterable, Optional, Tuple

from ordered_set import OrderedSet
from pydantic import validator

from onetl._internal import generate_temp_path  # noqa: WPS436
from onetl.base import BaseFileConnection
from onetl.core.file_result import FileSet
from onetl.core.file_uploader.upload_result import UploadResult
from onetl.exception import DirectoryNotFoundError, NotAFileError
from onetl.impl import (
    FailedLocalFile,
    FileWriteMode,
    FrozenModel,
    GenericOptions,
    LocalPath,
    RemotePath,
    path_repr,
)
from onetl.log import entity_boundary_log, log_lines, log_options, log_with_indent

log = getLogger(__name__)

# source, target, temp
UPLOAD_ITEMS_TYPE = OrderedSet[Tuple[LocalPath, RemotePath, Optional[RemotePath]]]


class FileUploader(FrozenModel):
    """Allows you to upload files to a remote source with specified file connection
    and parameters, and return an object with upload result summary.

    .. note::

        This class is used to upload files **only** from local directory to the remote one.

        It does NOT support direct file transfer between filesystems, like ``FTP -> SFTP``.
        You should use :ref:`file-downloader` + FileUploader to implement ``FTP -> local dir -> SFTP``.

    Parameters
    ----------
    connection : :obj:`onetl.connection.FileConnection`
        Class which contains File system connection properties. See in FileConnection section.

    target_path : os.PathLike or str
        Remote path where want you upload files to

    local_path : os.PathLike or str, optional, default: ``None``
        The local directory from which the data is loaded.

        Could be ``None``, but only if you pass absolute file paths directly to
        :obj:`~run` method

    temp_path : os.PathLike or str, optional, default: ``None``
        If set, this path will be used for uploading a file, and then renaming it to the target file path.
        If ``None`` (default since v0.5.0) is passed, files are uploaded directly to ``target_path``.

        .. warning::

            In case of production ETL pipelines, please set a value for ``temp_path`` (NOT ``None``).
            This allows to properly handle upload interruption,
            without creating half-uploaded files in the target,
            because unlike file upload, ``rename`` call is atomic.

        .. warning::

            In case of connections like SFTP or FTP, which can have multiple underlying filesystems,
            please pass ``temp_path`` path on the SAME filesystem as ``target_path``.
            Otherwise instead of ``rename``, remote OS will move file between filesystems,
            which is NOT atomic operation.

    options : :obj:`~FileUploader.Options` | dict | None, default: ``None``
        File upload options. See :obj:`~FileUploader.Options`

    Examples
    --------
    Simple Uploader creation

    .. code:: python

        from onetl.connection import HDFS
        from onetl.core import FileUploader

        hdfs = HDFS(...)

        uploader = FileUploader(
            connection=hdfs,
            target_path="/path/to/remote/source",
        )

    Uploader with all parameters

    .. code:: python

        from onetl.connection import HDFS
        from onetl.core import FileUploader

        hdfs = HDFS(...)

        uploader = FileUploader(
            connection=hdfs,
            target_path="/path/to/remote/source",
            temp_path="/user/onetl",
            local_path="/some/local/directory",
            options=FileUploader.Options(delete_local=True, mode="overwrite"),
        )

    """

    class Options(GenericOptions):
        """File uploader options"""

        mode: FileWriteMode = FileWriteMode.ERROR
        """
        How to handle existing files in the target directory.

        Possible values:
            * ``error`` (default) - do nothing, mark file as failed
            * ``ignore`` - do nothing, mark file as ignored
            * ``overwrite`` - replace existing file with a new one
            * ``delete_all`` - delete local directory content before downloading files
        """

        delete_local: bool = False
        """
        If ``True``, remove local file after successful download.

        If download failed, file will left intact.
        """

    connection: BaseFileConnection

    target_path: RemotePath

    local_path: Optional[LocalPath] = None
    temp_path: Optional[RemotePath] = None

    options: Options = Options()

    @validator("local_path", pre=True, always=True)
    def resolve_local_path(cls, local_path):
        return LocalPath(local_path).resolve() if local_path else None

    @validator("target_path", pre=True, always=True)
    def check_target_path(cls, target_path):
        return RemotePath(target_path)

    @validator("temp_path", pre=True, always=True)
    def check_temp_path(cls, temp_path):
        return RemotePath(temp_path) if temp_path else None

    def run(self, files: Iterable[str | os.PathLike] | None = None) -> UploadResult:
        """
        Method for uploading files to remote host.

        Parameters
        ----------

        files : Iterator[str | os.PathLike] | None, default ``None``
            File list to upload.

            If empty, upload files from ``local_path``.

        Returns
        -------
        uploaded_files : :obj:`onetl.core.file_uploader.upload_result.UploadResult`

            Upload result object

        Raises
        -------
        DirectoryNotFoundError

            ``local_path`` does not found

        NotADirectoryError

            ``local_path`` is not a directory

        ValueError

            File in ``files`` argument does not match ``local_path``

        Examples
        --------

        Upload files and get result

        .. code:: python

            from onetl.impl import (
                RemoteFile,
                LocalPath,
            )
            from onetl.core import FileUploader

            uploader = FileUploader(local_path="/local", target_path="/remote", ...)

            uploaded_files = uploader.run(
                [
                    "/local/file1",
                    "/local/file2",
                    "/failed/file",
                    "/existing/file",
                    "/missing/file",
                ]
            )

            # or without the list of files

            uploaded_files = uploader.run()

            assert uploaded_files.successful == {
                RemoteFile("/remote/file1"),
                RemoteFile("/remote/file2"),
            }
            assert uploaded_files.failed == {FailedLocalFile("/failed/file")}
            assert uploaded_files.skipped == {LocalPath("/existing/file")}
            assert uploaded_files.missing == {LocalPath("/missing/file")}
        """

        if files is None and not self.local_path:
            raise ValueError("Neither file list nor ``local_path`` are passed")

        self._log_options(files)

        # Check everything
        if self.local_path:
            self._check_local_path()

        self.connection.check()
        log_with_indent("")

        self.connection.mkdir(self.target_path)

        if files is None:
            log.info("|%s| File list is not passed to `run` method", self.__class__.__name__)
            files = self.view_files()

        if not files:
            log.info("|%s| No files to upload!", self.__class__.__name__)
            return UploadResult()

        current_temp_dir: RemotePath | None = None
        if self.temp_path:
            current_temp_dir = generate_temp_path(self.temp_path)

        to_upload = self._validate_files(files, current_temp_dir=current_temp_dir)

        # remove folder only after everything is checked
        if self.options.mode == FileWriteMode.DELETE_ALL:
            self.connection.rmdir(self.target_path, recursive=True)
            self.connection.mkdir(self.target_path)

        if current_temp_dir:
            current_temp_dir = self.connection.mkdir(current_temp_dir)

        result = self._upload_files(to_upload)

        if current_temp_dir:
            self._remove_temp_dir(current_temp_dir)

        self._log_result(result)
        return result

    def view_files(self) -> FileSet[LocalPath]:
        """
        Get file list in the ``local_path``

        Raises
        -------
        DirectoryNotFoundError

            ``local_path`` does not found

        NotADirectoryError

            ``local_path`` is not a directory

        Returns
        -------
        FileSet[LocalPath]
            Set of files in ``local_path``

        Examples
        --------

        View files

        .. code:: python

            from onetl.impl import LocalPath
            from onetl.core import FileUploader

            uploader = FileUploader(local_path="/local/path", ...)

            view_files = uploader.view_files()

            assert view_files == {
                LocalPath("/local/path/file1.txt"),
                LocalPath("/local/path/file3.txt"),
                LocalPath("/local/path/nested/file3.txt"),
            }
        """

        log.info("|Local FS| Getting files list from path '%s'", self.local_path)

        self._check_local_path()
        result = FileSet()

        try:
            for root, dirs, files in os.walk(self.local_path):
                log.debug("|Local FS| Listing dir '%s': %d dirs, %d files", root, len(dirs), len(files))
                result.update(LocalPath(root) / file for file in files)
        except Exception as e:
            raise RuntimeError(
                f"Couldn't read directory tree from local dir '{self.local_path}'",
            ) from e

        return result

    def _log_options(self, files: Iterable[str | os.PathLike] | None = None) -> None:
        entity_boundary_log(msg="FileUploader starts")

        log.info("|Local FS| -> |%s| Uploading files using parameters:'", self.connection.__class__.__name__)
        log_with_indent("local_path = %s", f"'{self.local_path}'" if self.local_path else "None")
        log_with_indent("target_path = '%s'", self.target_path)
        log_with_indent("temp_path = '%s'", f"'{self.temp_path}'" if self.temp_path else "None")

        log_options(self.options.dict(by_alias=True))

        if self.options.delete_local:
            log.warning("|%s| LOCAL FILES WILL BE PERMANENTLY DELETED AFTER UPLOADING !!!", self.__class__.__name__)

        if self.options.mode == FileWriteMode.DELETE_ALL:
            log.warning("|%s| TARGET DIRECTORY WILL BE CLEANED UP BEFORE UPLOADING FILES !!!", self.__class__.__name__)

        if files and self.local_path:
            log.warning(
                "|%s| Passed both ``local_path`` and files list at the same time. Using explicit files list",
                self.__class__.__name__,
            )

    def _validate_files(  # noqa: WPS231
        self,
        local_files: Iterable[os.PathLike | str],
        current_temp_dir: RemotePath | None,
    ) -> UPLOAD_ITEMS_TYPE:
        result = OrderedSet()

        for file in local_files:
            local_file_path = LocalPath(file)
            local_file = local_file_path
            tmp_file: RemotePath | None = None

            if not self.local_path:
                # Upload into a flat structure
                if not local_file_path.is_absolute():
                    raise ValueError("Cannot pass relative file path with empty ``local_path``")

                filename = local_file_path.name
                target_file = self.target_path / filename
                if current_temp_dir:
                    tmp_file = current_temp_dir / filename  # noqa: WPS220
            else:
                # Upload according to source folder structure
                if self.local_path in local_file_path.parents:
                    # Make relative remote path
                    target_file = self.target_path / local_file_path.relative_to(self.local_path)
                    if current_temp_dir:
                        tmp_file = current_temp_dir / local_file_path.relative_to(self.local_path)  # noqa: WPS220
                elif not local_file_path.is_absolute():
                    # Passed path is already relative
                    local_file = self.local_path / local_file_path
                    target_file = self.target_path / local_file_path
                    if current_temp_dir:
                        tmp_file = current_temp_dir / local_file_path  # noqa: WPS220
                else:
                    # Wrong path (not relative path and source path not in the path to the file)
                    raise ValueError(f"File path '{local_file}' does not match source_path '{self.local_path}'")

            if local_file.exists() and not local_file.is_file():
                raise NotAFileError(f"{path_repr(local_file)} is not a file")

            result.add((local_file, target_file, tmp_file))

        return result

    def _check_local_path(self):
        if not self.local_path.exists():
            raise DirectoryNotFoundError(f"'{self.local_path}' does not exist")

        if not self.local_path.is_dir():
            raise NotADirectoryError(f"{path_repr(self.local_path)} is not a directory")

    def _upload_files(self, to_upload: UPLOAD_ITEMS_TYPE) -> UploadResult:
        total_files = len(to_upload)
        files = FileSet(item[0] for item in to_upload)

        log.info("|%s| Files to be uploaded:", self.__class__.__name__)
        log_lines(str(files))
        log_with_indent("")
        log.info("|%s| Starting the upload process", self.__class__.__name__)

        result = UploadResult()
        for i, (local_file, target_file, tmp_file) in enumerate(to_upload):
            log.info("|%s| Uploading file %d of %d", self.__class__.__name__, i + 1, total_files)
            log_with_indent("from = '%s'", local_file)
            if tmp_file:
                log_with_indent("temp = '%s'", tmp_file)
            log_with_indent("to = '%s'", target_file)

            self._upload_file(local_file, target_file, tmp_file, result)

        return result

    def _upload_file(  # noqa: WPS231
        self,
        local_file: LocalPath,
        target_file: RemotePath,
        tmp_file: RemotePath | None,
        result: UploadResult,
    ) -> None:
        if not local_file.exists():
            log.warning("|%s| Missing file '%s', skipping", self.__class__.__name__, local_file)
            result.missing.add(local_file)
            return

        try:
            replace = False
            if self.connection.path_exists(target_file):
                file = self.connection.get_file(target_file)
                if self.options.mode == FileWriteMode.ERROR:
                    raise FileExistsError(f"File {path_repr(file)} already exists")

                if self.options.mode == FileWriteMode.IGNORE:
                    log.warning("|%s| File %s already exists, skipping", self.__class__.__name__, path_repr(file))
                    result.skipped.add(local_file)
                    return

                replace = True

            if tmp_file:
                # Files are loaded to temporary directory before moving them to target dir.
                # This prevents operations with partly uploaded files

                self.connection.upload_file(local_file, tmp_file)
                uploaded_file = self.connection.rename_file(tmp_file, target_file, replace=replace)
            else:
                # Direct upload
                uploaded_file = self.connection.upload_file(local_file, target_file, replace=replace)

            if self.options.delete_local:
                local_file.unlink()
                log.warning("|LocalFS| Successfully removed file %s", local_file)

            result.successful.add(uploaded_file)

        except Exception as e:
            log.exception("|%s| Couldn't upload file to target dir: %s", self.__class__.__name__, e, exc_info=False)
            result.failed.add(FailedLocalFile(path=local_file, exception=e))

    def _remove_temp_dir(self, temp_dir: RemotePath) -> None:
        try:
            self.connection.rmdir(temp_dir, recursive=True)
        except Exception:
            log.exception("|%s| Error while removing temp directory", self.__class__.__name__)

    def _log_result(self, result: UploadResult) -> None:
        log.info("")
        log.info("|%s| Upload result:", self.__class__.__name__)
        log_lines(str(result))
        entity_boundary_log(msg=f"{self.__class__.__name__} ends", char="-")
