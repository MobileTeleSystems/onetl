from __future__ import annotations

import os
import uuid
from dataclasses import InitVar, dataclass, field
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterable, Sized

from onetl.base import BaseFileConnection
from onetl.connection.file_connection.file_connection import (
    FileConnection,
    FileWriteMode,
)
from onetl.core.file_uploader.upload_result import UploadResult
from onetl.impl.failed_local_file import FailedLocalFile
from onetl.log import LOG_INDENT, entity_boundary_log, log_with_indent

log = getLogger(__name__)


@dataclass
class FileUploader:
    """Class specifies remote file source where you can upload files.

    Parameters
    ----------
    connection : :obj:`onetl.connection.FileConnection`
        Class which contains File system connection properties. See in FileConnection section.

    target_path : str
        Path on remote source where you upload files.

    temp_path : str, default: ``/tmp``
        Remote path where files uploaded firstly

        Default value: ``/tmp/``

    options : Options | dict | None, default: ``None``
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
            temp_path="/home/onetl",
        )

    """

    connection: BaseFileConnection

    target_path: InitVar[str | os.PathLike]
    _target_path: PurePosixPath = field(init=False)

    options: FileConnection.Options | dict | None = None
    _options: FileConnection.Options = field(init=False)

    temp_path: InitVar[str | os.PathLike] = field(default="/tmp")
    _temp_path: PurePosixPath = field(init=False)

    def __post_init__(self, target_path: str | os.PathLike, temp_path: str | os.PathLike):
        self._target_path = PurePosixPath(target_path)
        self._temp_path = PurePosixPath(temp_path)

        self._options = self.options or self.connection.Options()

        if isinstance(self.options, dict):
            self._options = self.connection.Options.parse_obj(self.options)

    def run(self, files: Iterable[str | os.PathLike]) -> UploadResult:  # noqa:WPS231 NOSONAR
        """
        Method for uploading files to remote host.

        Parameters
        ----------
        files : Iterable[str | os.PathLike]
            List of files on local storage

        Returns
        -------
        uploaded_files : :obj:`onetl.core.file_uploader.upload_result.UploadResult`

            Upload result object

        Examples
        --------

        Upload files and get result

        .. code:: python

            from pathlib import Path, PurePath
            from onetl.impl import RemoteFile
            from onetl.core import FileUploader

            uploader = FileUploader(target_path="/remote", ...)

            uploaded_files = uploader.run(
                [
                    "/local/file1",
                    "/local/file2",
                    "/failed/file",
                    "/existing/file",
                    "/missing/file",
                ]
            )

            assert uploaded_files.success == {
                RemoteFile("/remote/file1"),
                RemoteFile("/remote/file2"),
            }
            assert uploaded_files.failed == {FailedLocalFile("/failed/file")}
            assert uploaded_files.skipped == {Path("/existing/file")}
            assert uploaded_files.missing == {PurePath("/missing/file")}
        """

        if not files:
            log.warning(" ")
            log.warning(f"|{self.__class__.__name__}| Files list is empty. Please, provide files to upload.")
            return UploadResult()

        entity_boundary_log(msg="FileUploader starts")
        connection_class_name = self.connection.__class__.__name__

        log.info(f"|Local FS| -> |{connection_class_name}| Uploading files to path: {self._target_path} ")
        log.info(f"|{self.__class__.__name__}| Parameters:")
        log.info(" " * LOG_INDENT + f"target_path = {self._target_path}")
        log.info(" " * LOG_INDENT + f"temp_path = {self._temp_path}")

        log.info(f"|{self.__class__.__name__}| Using connection:")
        log.info(" " * LOG_INDENT + f"type = {self.connection.__class__.__name__}")
        log.info(" " * LOG_INDENT + f"host = {self.connection.host}")
        log.info(" " * LOG_INDENT + f"user = {self.connection.user}")

        if self._options.delete_source:
            log.warning(" ")
            log.warning(f"|{self.__class__.__name__}| LOCAL FILES WILL BE PERMANENTLY DELETED AFTER UPLOADING !!!")

        # TODO:(@dypedchenk) discuss the need for a mode DELETE_ALL
        if self._options.mode == FileWriteMode.DELETE_ALL:
            self.connection.rmdir(self._target_path, recursive=True)

        current_temp_dir = self._temp_path / uuid.uuid4().hex
        log.info(f"|{self.__class__.__name__}| Starting uploading files")

        result = UploadResult()

        step_suffix = ""
        if isinstance(files, Sized):
            step_suffix = f" of {len(files)}"

        for i, file in enumerate(files):
            file_path = Path(file)

            tmp_file = current_temp_dir / file_path.name
            target_file = self._target_path / file_path.name

            log.info(f"|{self.__class__.__name__}| Uploading file {i+1}{step_suffix}")
            log.info(" " * LOG_INDENT + f"from = '{file_path}'")
            log.info(" " * LOG_INDENT + f"to = '{target_file}'")

            try:
                if not file_path.exists():
                    log.warning(f"|{self.__class__.__name__}| Missing file '{file_path}', skipping")
                    result.missing.add(file_path)
                    continue

                replace = False
                if self.connection.path_exists(target_file):
                    error_message = f"Target directory already contains file '{target_file}'"
                    if self._options.mode == FileWriteMode.ERROR:
                        raise FileExistsError(error_message)

                    if self._options.mode == FileWriteMode.IGNORE:
                        log.warning(f"|{self.__class__.__name__}| {error_message}, skipping")
                        result.skipped.add(file_path)
                        continue

                    replace = True
                    log.warning(f"|{self.__class__.__name__}| {error_message}, overwriting")

                self.connection.upload_file(file_path, tmp_file)

                # Files are loaded to temporary directory before moving them to target dir.
                # This prevents operations with partly uploaded files

                uploaded_file = self.connection.rename_file(tmp_file, target_file, replace=replace)

                # Remove files
                if self._options.delete_source:
                    file_path.unlink()

                result.success.add(uploaded_file)

            except Exception as e:
                log.exception(f"|{self.__class__.__name__}| Couldn't upload file to target dir: {e}", exc_info=False)
                result.failed.add(FailedLocalFile(path=file_path, exception=e))

        try:
            log.info(f"|{connection_class_name}| Removing temp directory: '{current_temp_dir}'")
            self.connection.rmdir(current_temp_dir, recursive=True)
        except Exception:
            log.exception(f"|{self.__class__.__name__}| Error while removing temp directory")

        log.info(f"|{self.__class__.__name__}| Upload result:")
        log_with_indent(str(result))
        entity_boundary_log(msg=f"{self.__class__.__name__} ends", char="-")

        return result
