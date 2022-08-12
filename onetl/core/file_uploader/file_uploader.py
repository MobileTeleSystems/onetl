from __future__ import annotations

import os
from dataclasses import InitVar, dataclass, field
from enum import Enum
from logging import getLogger
from typing import Iterable, Optional, Tuple

from ordered_set import OrderedSet
from pydantic import BaseModel

from onetl._internal import generate_temp_path  # noqa: WPS436
from onetl.base import BaseFileConnection
from onetl.core.file_result import FileSet
from onetl.core.file_uploader.upload_result import UploadResult
from onetl.exception import DirectoryNotFoundError, NotAFileError
from onetl.impl import FailedLocalFile, FileWriteMode, LocalPath, RemotePath
from onetl.log import entity_boundary_log, log_with_indent

log = getLogger(__name__)

# source, target, temp
UPLOAD_ITEMS_TYPE = OrderedSet[Tuple[LocalPath, RemotePath, Optional[RemotePath]]]


@dataclass
class FileUploader:
    """Class specifies remote file source where you can upload files.

    Parameters
    ----------
    connection : :obj:`onetl.connection.FileConnection`
        Class which contains File system connection properties. See in FileConnection section.

    target_path : os.PathLike or str
        Remote path where want you upload files to

    local_path : os.PathLike or str, optional, default: ``None``
        The local directory from which the data is loaded.

        Could be ``None``, but only if you pass absolute file paths directly to
        :obj:`onetl.core.file_uploader.file_uploader.FileUploader.run` method

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

    options : :obj:`onetl.core.file_uploader.file_uploader.FileUploader.Options` | dict | None, default: ``None``
        File upload options

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

    class Options(BaseModel):  # noqa: WPS431
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

        class Config:  # noqa: WPS431
            frozen = True

    connection: BaseFileConnection

    target_path: InitVar[str | os.PathLike]
    _target_path: RemotePath = field(init=False)

    local_path: InitVar[os.PathLike | str | None] = field(default=None)
    _local_path: LocalPath | None = field(init=False)

    temp_path: InitVar[str | os.PathLike] = field(default=None)
    _temp_path: RemotePath | None = field(init=False)

    options: InitVar[Options | dict | None] = field(default=None)
    _options: Options = field(init=False)

    def __post_init__(
        self,
        target_path: str | os.PathLike,
        local_path: str | os.PathLike | None,
        temp_path: str | os.PathLike,
        options: str | os.FileConnection.Options | dict | None,
    ):
        self._target_path = RemotePath(target_path)
        self._local_path = LocalPath(local_path).resolve() if local_path else None
        self._temp_path = RemotePath(temp_path) if temp_path else None
        self._options = options or self.Options()

        if isinstance(options, dict):
            self._options = self.Options.parse_obj(options)

    def run(self, files: Iterable[str | os.PathLike] | None = None) -> UploadResult:
        """
        Method for uploading files to remote host.

        Parameters
        ----------

        files : Iterator[str | os.PathLike] | None, default ``None``
            File collection to upload.

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

        if files is None and not self._local_path:
            raise ValueError("Neither file collection nor ``local_path`` are passed")

        self._log_options(files)

        # Check everything
        if self._local_path:
            self._check_local_path()

        self.connection.check()
        log.info("")

        self.connection.mkdir(self._target_path)

        if files is None:
            log.info(f"|{self.__class__.__name__}| File collection is not passed to `run` method")
            files = self.view_files()

        if not files:
            log.info(f"|{self.__class__.__name__}| No files to upload!")
            return UploadResult()

        current_temp_dir: RemotePath | None = None
        if self._temp_path:
            current_temp_dir = generate_temp_path(self._temp_path)

        to_upload = self._validate_files(files, current_temp_dir=current_temp_dir)

        # remove folder only after everything is checked
        if self._options.mode == FileWriteMode.DELETE_ALL:
            self.connection.rmdir(self._target_path, recursive=True)
            self.connection.mkdir(self._target_path)

        if current_temp_dir:
            self.connection.mkdir(current_temp_dir)

        result = self._upload_files(to_upload)

        if current_temp_dir:
            self._remove_temp_dir(current_temp_dir)

        self._log_result(result)
        return result

    def view_files(self) -> FileSet[LocalPath]:
        """
        Get file collection in the ``local_path``

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

        log.info(f"|Local FS| Getting files list from path '{self._local_path}'")

        self._check_local_path()
        result = FileSet()

        try:
            for root, dirs, files in os.walk(self._local_path):
                log.debug(f"|Local FS| Listing dir '{root}': {len(dirs)} dirs, {len(files)} files")
                result.update(LocalPath(root) / file for file in files)
        except Exception as e:
            raise RuntimeError(
                f"Couldn't read directory tree from local dir '{self._local_path}'",
            ) from e

        return result

    def _log_options(self, files: Iterable[str | os.PathLike] | None = None) -> None:
        entity_boundary_log(msg="FileUploader starts")

        log.info(f"|Local FS| -> |{self.connection.__class__.__name__}| Uploading files using parameters:'")
        local_path_str = f"'{self._local_path}'" if self._local_path else "None"
        log_with_indent(f"local_path = {local_path_str}")
        log_with_indent(f"target_path = '{self._target_path}'")
        if self._temp_path:
            log_with_indent(f"temp_path = '{self._temp_path}'")
        else:
            log_with_indent("temp_path = None")

        log.info("")
        log_with_indent("options:")
        for option, value in self._options.dict().items():
            value_wrapped = f"'{value}'" if isinstance(value, Enum) else repr(value)
            log_with_indent(f"    {option} = {value_wrapped}")
        log.info("")

        if self._options.delete_local:
            log.warning(f"|{self.__class__.__name__}| LOCAL FILES WILL BE PERMANENTLY DELETED AFTER UPLOADING !!!")

        if self._options.mode == FileWriteMode.DELETE_ALL:
            log.warning(f"|{self.__class__.__name__}| TARGET DIRECTORY WILL BE CLEANED UP BEFORE UPLOADING FILES !!!")

        if files and self._local_path:
            log.warning(
                f"|{self.__class__.__name__}| Passed both ``local_path`` and file collection at the same time. "
                "File collection will be used",
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

            if not self._local_path:
                # Upload into a flat structure
                if not local_file_path.is_absolute():
                    raise ValueError("Cannot pass relative file path with empty ``local_path``")

                filename = local_file_path.name
                target_file = self._target_path / filename
                if current_temp_dir:
                    tmp_file = current_temp_dir / filename  # noqa: WPS220
            else:
                # Upload according to source folder structure
                if self._local_path in local_file_path.parents:
                    # Make relative remote path
                    target_file = self._target_path / local_file_path.relative_to(self._local_path)
                    if current_temp_dir:
                        tmp_file = current_temp_dir / local_file_path.relative_to(self._local_path)  # noqa: WPS220
                elif not local_file_path.is_absolute():
                    # Passed path is already relative
                    local_file = self._local_path / local_file_path
                    target_file = self._target_path / local_file_path
                    if current_temp_dir:
                        tmp_file = current_temp_dir / local_file_path  # noqa: WPS220
                else:
                    # Wrong path (not relative path and source path not in the path to the file)
                    raise ValueError(f"File path '{local_file}' does not match source_path '{self._local_path}'")

            if local_file.exists() and not local_file.is_file():
                raise NotAFileError(f"|Local FS| '{local_file}' is not a file")

            result.add((local_file, target_file, tmp_file))

        return result

    def _check_local_path(self):
        if not self._local_path.exists():
            raise DirectoryNotFoundError(f"|Local FS| '{self._local_path}' does not exist")

        if not self._local_path.is_dir():
            raise NotADirectoryError(f"|Local FS| '{self._local_path}' is not a directory")

    def _upload_files(self, to_upload: UPLOAD_ITEMS_TYPE) -> UploadResult:
        total_files = len(to_upload)
        files = FileSet(item[0] for item in to_upload)

        log.info(f"|{self.__class__.__name__}| Files to be uploaded:")
        log_with_indent(str(files))
        log.info(f"|{self.__class__.__name__}| Starting the upload process")

        result = UploadResult()
        for i, (local_file, target_file, tmp_file) in enumerate(to_upload):
            log.info(f"|{self.__class__.__name__}| Uploading file {i+1} of {total_files}")
            log_with_indent(f"from = '{local_file}'")
            if tmp_file:
                log_with_indent(f"temp = '{tmp_file}'")
            log_with_indent(f"to = '{target_file}'")

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
            log.warning(f"|{self.__class__.__name__}| Missing file '{local_file}', skipping")
            result.missing.add(local_file)
            return

        try:
            replace = False
            if self.connection.path_exists(target_file):
                error_message = f"Target directory already contains file '{target_file}'"
                if self._options.mode == FileWriteMode.ERROR:
                    raise FileExistsError(error_message)

                if self._options.mode == FileWriteMode.IGNORE:
                    log.warning(f"|{self.__class__.__name__}| {error_message}, skipping")
                    result.skipped.add(local_file)
                    return

                replace = True
                log.warning(f"|{self.__class__.__name__}| {error_message}, overwriting")

            if tmp_file:
                # Files are loaded to temporary directory before moving them to target dir.
                # This prevents operations with partly uploaded files

                self.connection.upload_file(local_file, tmp_file)
                uploaded_file = self.connection.rename_file(tmp_file, target_file, replace=replace)
            else:
                # Direct upload
                uploaded_file = self.connection.upload_file(local_file, target_file, replace=replace)

            if self._options.delete_local:
                local_file.unlink()
                log.warning(f"|LocalFS| Successfully removed file '{local_file}'")

            result.successful.add(uploaded_file)

        except Exception as e:
            log.exception(f"|{self.__class__.__name__}| Couldn't upload file to target dir: {e}", exc_info=False)
            result.failed.add(FailedLocalFile(path=local_file, exception=e))

    def _remove_temp_dir(self, temp_dir: RemotePath) -> None:
        log.debug(f"|{self.connection.__class__.__name__}| Removing temp directory '{temp_dir}'")

        try:
            self.connection.rmdir(temp_dir, recursive=True)
        except Exception:
            log.exception(f"|{self.__class__.__name__}| Error while removing temp directory")

    def _log_result(self, result: UploadResult) -> None:
        log.info(f"|{self.__class__.__name__}| Upload result:")
        log_with_indent(str(result))
        entity_boundary_log(msg=f"{self.__class__.__name__} ends", char="-")
