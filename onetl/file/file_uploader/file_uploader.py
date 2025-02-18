# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from typing import Iterable, Optional, Tuple

from ordered_set import OrderedSet

try:
    from pydantic.v1 import PrivateAttr, validator
except (ImportError, AttributeError):
    from pydantic import PrivateAttr, validator  # type: ignore[no-redef, assignment]

from onetl._util.file import generate_temp_path
from onetl.base import BaseFileConnection
from onetl.base.path_protocol import PathWithStatsProtocol
from onetl.base.pure_path_protocol import PurePathProtocol
from onetl.exception import DirectoryNotFoundError, NotAFileError
from onetl.file.file_set import FileSet
from onetl.file.file_uploader.options import FileUploaderOptions
from onetl.file.file_uploader.result import UploadResult
from onetl.hooks import slot, support_hooks
from onetl.impl import (
    FailedLocalFile,
    FileExistBehavior,
    FrozenModel,
    LocalPath,
    RemotePath,
    path_repr,
)
from onetl.log import entity_boundary_log, log_lines, log_options, log_with_indent

log = logging.getLogger(__name__)

# source, target, temp
UPLOAD_ITEMS_TYPE = OrderedSet[Tuple[LocalPath, RemotePath, Optional[RemotePath]]]


class FileUploadStatus(Enum):
    SUCCESSFUL = 0
    FAILED = 1
    SKIPPED = 2
    MISSING = -1


@support_hooks
class FileUploader(FrozenModel):
    """Allows you to upload files to a remote source with specified file connection
    and parameters, and return an object with upload result summary. |support_hooks|

    .. note::

        This class is used to upload files **only** from local directory to the remote one.

        It does NOT support direct file transfer between filesystems, like ``FTP -> SFTP``.
        You should use :ref:`file-downloader` + FileUploader to implement ``FTP -> local dir -> SFTP``.

    .. warning::

        This class does **not** support read strategies.

    .. versionadded:: 0.1.0

    .. versionchanged:: 0.8.0
        Moved ``onetl.core.FileDownloader`` → ``onetl.file.FileDownloader``

    Parameters
    ----------
    connection : :obj:`onetl.connection.FileConnection`
        Class which contains File system connection properties. See :ref:`file-connections` section.

    target_path : os.PathLike or str
        Remote path where want you upload files to

    local_path : os.PathLike or str, optional, default: ``None``
        The local directory from which the data is loaded.

        Could be ``None``, but only if you pass absolute file paths directly to
        :obj:`~run` method

        .. versionadded:: 0.3.0

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

        .. versionchanged:: 0.5.0
            Default changed from ``/tmp`` to ``None``

    options : :obj:`~FileUploader.Options` | dict | None, default: ``None``
        File upload options. See :obj:`FileUploader.Options <onetl.file.file_uploader.options.FileUploaderOptions>`

    Examples
    --------

    .. tabs::

        .. code-tab:: py Minimal example

            from onetl.connection import HDFS
            from onetl.file import FileUploader

            hdfs = HDFS(...)

            uploader = FileUploader(
                connection=hdfs,
                target_path="/path/to/remote/source",
            )

        .. code-tab:: py Full example

            from onetl.connection import HDFS
            from onetl.file import FileUploader

            hdfs = HDFS(...)

            uploader = FileUploader(
                connection=hdfs,
                target_path="/path/to/remote/source",
                temp_path="/user/onetl",
                local_path="/some/local/directory",
                options=FileUploader.Options(delete_local=True, if_exists="overwrite"),
            )
    """

    Options = FileUploaderOptions

    connection: BaseFileConnection

    target_path: RemotePath

    local_path: Optional[LocalPath] = None
    temp_path: Optional[RemotePath] = None

    options: FileUploaderOptions = FileUploaderOptions()

    _connection_checked: bool = PrivateAttr(default=False)

    @slot
    def run(self, files: Iterable[str | os.PathLike] | None = None) -> UploadResult:
        """
        Method for uploading files to remote host. |support_hooks|

        .. versionadded:: 0.1.0

        Parameters
        ----------

        files : Iterator[str | os.PathLike] | None, default ``None``
            File list to upload.

            If empty, upload files from ``local_path``.

        Returns
        -------
        :obj:`UploadResult <onetl.file.file_uploader.upload_result.UploadResult>`

            Upload result object

        Raises
        ------
        :obj:`onetl.exception.DirectoryNotFoundError`

            ``local_path`` does not found

        NotADirectoryError

            ``local_path`` is not a directory

        ValueError

            File in ``files`` argument does not match ``local_path``

        Examples
        --------

        Upload files from ``local_path`` to ``target_path``:

        >>> from onetl.file import FileUploader
        >>> uploader = FileUploader(local_path="/local", target_path="/remote", ...)
        >>> upload_result = uploader.run()
        >>> upload_result
        UploadResult(
            successful=FileSet([
                RemoteFile("/remote/file1"),
                RemoteFile("/remote/file2"),
                # directory structure is preserved
                RemoteFile("/remote/nested/path/file3")
            ]),
            failed=FileSet([
                FailedLocalFile("/local/failed.file"),
            ]),
            skipped=FileSet([
                LocalPath("/local/already.exists"),
            ]),
            missing=FileSet([
                LocalPath("/local/missing.file"),
            ]),
        )

        Upload only certain files from ``local_path``:

        >>> from onetl.file import FileUploader
        >>> uploader = FileUploader(local_path="/local", target_path="/remote", ...)
        >>> # paths could be relative or absolute, but all should be in "/local"
        >>> upload_result = uploader.run(
        ...     [
        ...         "/local/file1",
        ...         "/local/nested/path/file3",
        ...         # excluding "/local/file2",
        ...     ]
        ... )
        >>> upload_result
        UploadResult(
            successful=FileSet([
                RemoteFile("/remote/file1"),
                # directory structure is preserved
                RemoteFile("/remote/nested/path/file3"),
            ]),
            failed=FileSet([]),
            skipped=FileSet([]),
            missing=FileSet([]),
        )

        Upload only certain files from any folder:

        >>> from onetl.file import FileUploader
        >>> uploader = FileUploader(target_path="/remote", ...)  # no local_path set
        >>> # only absolute paths
        >>> upload_result = uploader.run(
        ...     [
        ...         "/local/file1.txt",
        ...         "/any/nested/path/file3.txt",
        ...     ]
        ... )
        >>> upload_result
        UploadResult(
            successful=FileSet([
                RemoteFile("/remote/file1.txt"),
                # directory structure is NOT preserved without local_path
                RemoteFile("/remote/file3.txt"),
            ]),
            failed=FileSet([]),
            skipped=FileSet([]),
            missing=FileSet([]),
        )
        """

        entity_boundary_log(log, f"{self.__class__.__name__}.run() starts")

        if files is None and not self.local_path:
            raise ValueError("Neither file list nor `local_path` are passed")

        if not self._connection_checked:
            self._log_parameters(files)

            if self.local_path:
                self._check_local_path()
            self.connection.check()
            self._connection_checked = True

        self.connection.create_dir(self.target_path)

        if files is None:
            log.debug("|%s| File list is not passed to `run` method", self.__class__.__name__)
            files = self.view_files()

        if not files:
            log.info("|%s| No files to upload!", self.__class__.__name__)
            return UploadResult()

        current_temp_dir: RemotePath | None = None
        if self.temp_path:
            current_temp_dir = generate_temp_path(self.temp_path)

        to_upload = self._validate_files(files, current_temp_dir=current_temp_dir)

        # remove folder only after everything is checked
        if self.options.if_exists == FileExistBehavior.REPLACE_ENTIRE_DIRECTORY:
            self.connection.remove_dir(self.target_path, recursive=True)
            self.connection.create_dir(self.target_path)

        if current_temp_dir:
            current_temp_dir = self.connection.create_dir(current_temp_dir)

        result = self._upload_files(to_upload)

        if current_temp_dir:
            self._remove_temp_dir(current_temp_dir)

        self._log_result(result)
        entity_boundary_log(log, f"{self.__class__.__name__}.run() ends", char="-")
        return result

    @slot
    def view_files(self) -> FileSet[LocalPath]:
        """
        Get file list in the ``local_path``. |support_hooks|

        .. versionadded:: 0.3.0

        Raises
        ------
        :obj:`onetl.exception.DirectoryNotFoundError`

            ``local_path`` does not found

        NotADirectoryError

            ``local_path`` is not a directory

        Returns
        -------
        FileSet[LocalPath]
            Set of files in ``local_path``

        Examples
        --------

        View files:

        >>> from onetl.file import FileUploader
        >>> uploader = FileUploader(local_path="/local", ...)
        >>> uploader.view_files()
        FileSet([
            LocalPath("/local/file1.txt"),
            LocalPath("/local/file3.txt"),
            LocalPath("/local/nested/path/file3.txt"),
        ])
        """

        if not self.local_path:
            raise ValueError("Cannot call `.view_files()` without `local_path`")

        log.debug("|Local FS| Getting files list from path '%s'", self.local_path)

        if not self._connection_checked:
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

    @validator("local_path", pre=True, always=True)
    def _resolve_local_path(cls, local_path):
        return LocalPath(local_path).resolve() if local_path else None

    @validator("target_path", pre=True, always=True)
    def _validate_target_path(cls, target_path):
        return RemotePath(target_path)

    @validator("temp_path", pre=True, always=True)
    def _validate_temp_path(cls, temp_path):
        return RemotePath(temp_path) if temp_path else None

    def _log_parameters(self, files: Iterable[str | os.PathLike] | None = None) -> None:
        log.info("|Local FS| -> |%s| Uploading files using parameters:'", self.connection.__class__.__name__)
        log_with_indent(log, "local_path = %s", f"'{self.local_path}'" if self.local_path else "None")
        log_with_indent(log, "target_path = '%s'", self.target_path)
        log_with_indent(log, "temp_path = %s", f"'{self.temp_path}'" if self.temp_path else "None")
        log_options(log, self.options.dict(by_alias=True))

        if self.options.delete_local:
            log.warning("|%s| LOCAL FILES WILL BE PERMANENTLY DELETED AFTER UPLOADING !!!", self.__class__.__name__)

        if self.options.if_exists == FileExistBehavior.REPLACE_ENTIRE_DIRECTORY:
            log.warning("|%s| TARGET DIRECTORY WILL BE CLEANED UP BEFORE UPLOADING FILES !!!", self.__class__.__name__)

        if files and self.local_path:
            log.warning(
                "|%s| Passed both `local_path` and files list at the same time. Using explicit files list",
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
                    raise ValueError("Cannot pass relative file path with empty `local_path`")

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
        files = FileSet(item[0] for item in to_upload)
        log.info("|%s| Files to be uploaded:", self.__class__.__name__)
        log_lines(log, str(files))
        log_with_indent(log, "")
        log.info("|%s| Starting the upload process...", self.__class__.__name__)

        self._create_dirs(to_upload)

        result = UploadResult()
        for status, file in self._bulk_upload(to_upload):
            if status == FileUploadStatus.SUCCESSFUL:
                result.successful.add(file)
            elif status == FileUploadStatus.FAILED:
                result.failed.add(file)
            elif status == FileUploadStatus.SKIPPED:
                result.skipped.add(file)
            elif status == FileUploadStatus.MISSING:
                result.missing.add(file)

        return result

    def _create_dirs(
        self,
        to_upload: UPLOAD_ITEMS_TYPE,
    ) -> None:
        """
        Create all parent paths before uploading files
        This is required to avoid errors then multiple threads create the same dir
        """
        parent_paths = OrderedSet()
        for _, target_file, tmp_file in to_upload:
            parent_paths.add(target_file.parent)
            if tmp_file:
                parent_paths.add(tmp_file.parent)

        for parent_path in parent_paths:
            self.connection.create_dir(parent_path)

    def _bulk_upload(
        self,
        to_upload: UPLOAD_ITEMS_TYPE,
    ) -> list[tuple[FileUploadStatus, PurePathProtocol | PathWithStatsProtocol]]:
        workers = self.options.workers
        files_count = len(to_upload)
        result = []

        real_workers = workers
        if files_count < workers:
            log.debug(
                "|%s| Asked for %d workers, but there are only %d files",
                self.__class__.__name__,
                workers,
                files_count,
            )
            real_workers = files_count

        if real_workers > 1:
            log.debug("|%s| Using ThreadPoolExecutor with %d workers", self.__class__.__name__, real_workers)
            with ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix=self.__class__.__name__,
            ) as executor:
                futures = [
                    executor.submit(self._upload_file, local_file, target_file, tmp_file)
                    for local_file, target_file, tmp_file in to_upload
                ]
                for future in as_completed(futures):
                    result.append(future.result())
        else:
            log.debug("|%s| Using plain old for-loop", self.__class__.__name__)
            for local_file, target_file, tmp_file in to_upload:
                result.append(
                    self._upload_file(
                        local_file,
                        target_file,
                        tmp_file,
                    ),
                )

        return result

    def _upload_file(  # noqa: WPS231
        self,
        local_file: LocalPath,
        target_file: RemotePath,
        tmp_file: RemotePath | None,
    ) -> tuple[FileUploadStatus, PurePathProtocol | PathWithStatsProtocol]:
        if tmp_file:
            log.info(
                "|%s| Uploading file '%s' to '%s' (via tmp '%s')",
                self.__class__.__name__,
                local_file,
                target_file,
                tmp_file,
            )
        else:
            log.info("|%s| Uploading file '%s' to '%s'", self.__class__.__name__, local_file, target_file)

        if not local_file.exists():
            log.warning("|%s| Missing file '%s', skipping", self.__class__.__name__, local_file)
            return FileUploadStatus.MISSING, local_file

        try:
            replace = False
            if self.connection.path_exists(target_file):
                file = self.connection.resolve_file(target_file)
                if self.options.if_exists == FileExistBehavior.ERROR:
                    raise FileExistsError(f"File {path_repr(file)} already exists")

                if self.options.if_exists == FileExistBehavior.IGNORE:
                    log.warning("|%s| File %s already exists, skipping", self.__class__.__name__, path_repr(file))
                    return FileUploadStatus.SKIPPED, local_file

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
                log.warning("|Local FS| Successfully removed file %s", local_file)

            return FileUploadStatus.SUCCESSFUL, uploaded_file

        except Exception as e:
            if log.isEnabledFor(logging.DEBUG):
                log.exception("|%s| Couldn't upload file to target dir", self.__class__.__name__, exc_info=e)
            else:
                log.exception("|%s| Couldn't upload file to target dir: %s", self.__class__.__name__, e, exc_info=False)

            return FileUploadStatus.FAILED, FailedLocalFile(path=local_file, exception=e)

    def _remove_temp_dir(self, temp_dir: RemotePath) -> None:
        try:
            self.connection.remove_dir(temp_dir, recursive=True)
        except Exception:
            log.exception("|%s| Error while removing temp directory", self.__class__.__name__)

    def _log_result(self, result: UploadResult) -> None:
        log.info("")
        log.info("|%s| Upload result:", self.__class__.__name__)
        log_lines(log, str(result))
