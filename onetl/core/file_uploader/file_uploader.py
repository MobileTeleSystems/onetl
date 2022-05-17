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
from onetl.core.file_result import FileSet
from onetl.core.file_uploader.upload_result import UploadResult
from onetl.exception import DirectoryNotFoundError
from onetl.impl import FailedLocalFile
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

    local_path : str
        The local directory from which the data is loaded.

        Default value: None

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
            local_path="/some/local/directory",
        )

    """

    connection: BaseFileConnection

    target_path: InitVar[str | os.PathLike]
    _target_path: PurePosixPath = field(init=False)

    local_path: InitVar[os.PathLike | str | None] = field(default=None)
    _local_path: Path | None = field(init=False)

    temp_path: InitVar[str | os.PathLike] = field(default="/tmp")
    _temp_path: PurePosixPath = field(init=False)

    options: InitVar[FileConnection.Options | dict | None] = field(default=None)
    _options: FileConnection.Options = field(init=False)

    def __post_init__(
        self,
        target_path: str | os.PathLike,
        local_path: str | os.PathLike | None,
        temp_path: str | os.PathLike,
        options: str | os.FileConnection.Options | dict | None,
    ):
        self._target_path = PurePosixPath(target_path)
        self._local_path = Path(local_path) if local_path else None
        self._temp_path = PurePosixPath(temp_path)
        self._options = options or self.connection.Options()

        if isinstance(options, dict):
            self._options = self.connection.Options.parse_obj(options)

    def run(self, files: Iterable[str | os.PathLike] | None = None) -> UploadResult:  # noqa:WPS231, WPS238 NOSONAR
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

            from pathlib import Path, PurePath
            from onetl.impl import RemoteFile
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

            assert uploaded_files.success == {
                RemoteFile("/remote/file1"),
                RemoteFile("/remote/file2"),
            }
            assert uploaded_files.failed == {FailedLocalFile("/failed/file")}
            assert uploaded_files.skipped == {Path("/existing/file")}
            assert uploaded_files.missing == {PurePath("/missing/file")}
        """

        if files is None and not self._local_path:
            raise ValueError("Neither file collection nor ``local_path`` are passed")

        # Log all options
        entity_boundary_log(msg="FileUploader starts")
        connection_class_name = self.connection.__class__.__name__

        log.info(f"|Local FS| -> |{connection_class_name}| Uploading files to path: {self._target_path} ")
        log.info(f"|{self.__class__.__name__}| Parameters:")
        log.info(" " * LOG_INDENT + f"target_path = {self._target_path}")
        log.info(" " * LOG_INDENT + f"temp_path = {self._temp_path}")

        if files and self._local_path:
            log.warning(
                f"|{self.__class__.__name__}| Passed both ``local_path`` and file collection at the same time. "
                "File collection will be used",
            )

        if self._options.delete_source:
            log.warning(" ")
            log.warning(f"|{self.__class__.__name__}| LOCAL FILES WILL BE PERMANENTLY DELETED AFTER UPLOADING !!!")

        # Check everything
        if self._local_path:
            self._check_local_path()

        self.connection.check()
        log.info("")

        self.connection.mkdir(self._target_path)

        if files is None:
            log.info(f"|{self.__class__.__name__}| File collection is not passed to `run` method")
            files = self.view_files()

        # TODO:(@dypedchenk) discuss the need for a mode DELETE_ALL
        if self._options.mode == FileWriteMode.DELETE_ALL:
            log.warning(f"|{self.__class__.__name__}| TARGET DIRECTORY WILL BE CLEANED UP BEFORE UPLOADING FILES !!!")
            self.connection.rmdir(self._target_path, recursive=True)
            self.connection.mkdir(self._target_path)

        step_suffix = ""
        if isinstance(files, Sized):
            step_suffix = f" of {len(files)}"

        current_temp_dir = self._temp_path / uuid.uuid4().hex
        result = UploadResult()

        log.info(f"|{self.__class__.__name__}| Start uploading files")
        self.connection.mkdir(current_temp_dir)

        for i, (file, target_file, tmp_file) in enumerate(  # noqa: WPS352
            self._validate_files_list(local_files=files, current_temp_dir=current_temp_dir),
        ):
            file_path = Path(file)

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
                    log.warning(f"|LocalFS| Successfully removed file: '{file_path}'")

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

    def view_files(self) -> FileSet[Path]:
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
        FileSet[Path]
            Set of files in ``local_path``

        Examples
        --------

        View files

        .. code:: python

            from pathlib import Path
            from onetl.core import FileUploader

            uploader = FileUploader(local_path="/local/path", ...)

            view_files = uploader.view_files()

            assert view_files == {
                Path("/local/path/file1.txt"),
                Path("/local/path/file3.txt"),
                Path("/local/path/nested/file3.txt"),
            }
        """

        log.info(f"|{self.connection.__class__.__name__}| Getting files list from path: '{self._local_path}'")

        self._check_local_path()
        result = FileSet()

        try:
            for root, dirs, files in os.walk(self._local_path):
                log.debug(
                    f"|{self.connection.__class__.__name__}| "
                    f"Listing dir '{root}', dirs: {len(dirs)} files: {len(files)}",
                )

                result.update(Path(root) / file for file in files)
        except Exception as e:
            raise RuntimeError(
                f"Couldn't read directory tree from local dir {self._local_path}",
            ) from e

        return result

    def _validate_files_list(
        self,
        local_files: list[os.PathLike | str],
        current_temp_dir: os.PathLike | str,
    ) -> list[tuple[Path, PurePosixPath, PurePosixPath]]:
        valid_file_list = []
        tmp_file: PurePosixPath

        for file in local_files:
            file_path = Path(file)

            if not self._local_path:
                # Upload into a flat structure
                if not file_path.is_absolute():
                    raise ValueError("Cannot pass relative file path with empty ``local_path``")

                filename = file_path.name
                target_file = self._target_path / filename
                tmp_file = current_temp_dir / filename
            else:
                # Upload according to source folder structure
                if self._local_path in file_path.parents:
                    # Make relative remote path
                    target_file = self._target_path / file_path.relative_to(self._local_path)
                    tmp_file = current_temp_dir / file_path.relative_to(self._local_path)
                elif not file_path.is_absolute():
                    # Passed already relative path
                    relative_path = file_path
                    file_path = self._local_path / relative_path
                    target_file = self._target_path / relative_path
                    tmp_file = current_temp_dir / relative_path
                else:
                    # Wrong path (not relative path and source path not in the path to the file)
                    raise ValueError(f"File path '{file_path}' does not match source_path '{self._local_path}'")
            valid_file_list.append((file_path, target_file, tmp_file))

        return valid_file_list

    def _check_local_path(self):
        if not self._local_path.exists():
            raise DirectoryNotFoundError(f"'{self._local_path}' does not exist")

        if not self._local_path.is_dir():
            raise NotADirectoryError(f"'{self._local_path}' is not a directory")
