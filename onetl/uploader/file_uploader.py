from __future__ import annotations

import uuid
import os
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path, PosixPath

import humanize

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.connection.connection_helpers import decorated_log, LOG_INDENT

log = getLogger(__name__)


@dataclass
class FileUploader:
    """Class specifies remote file source where you can upload files.

    Parameters
    ----------
    connection : onetl.connection.file_connection.FileConnection
        Class which contain File system connection properties. See in FileConnection section.
    target_path : str
        Path on remote source where you upload files.
    temp_path : str, optional, default: ``/tmp/{uuid.uuid4()}``
        Remote path where files uploaded firstly

        Default value: ``/tmp/{uuid.uuid4()}``

    Examples
    --------
    Simple Uploader creation

    .. code::

        from onetl.uploader import FileUploader
        from onetl.connection.file_connection import HDFS

        hdfs = HDFS(...)

        uploader = FileUploader(
            connection=hdfs,
            target_path="/path/to/remote/source",
        )

    Uploader with all parameters

    .. code::

        from onetl.uploader import FileUploader
        from onetl.connection.file_connection import HDFS

        hdfs = HDFS(...)

        uploader = FileUploader(
            connection=hdfs,
            target_path="/path/to/remote/source",
            temp_path="/home/onetl"
        )

    """

    connection: FileConnection
    target_path: str | os.PathLike
    # Remote temporary path to upload files
    temp_path: str | os.PathLike | None = "/tmp/{}"  # NOSONAR

    def __post_init__(self):
        self.target_path = PosixPath(self.target_path)

    def run(self, files_list: list[str | os.PathLike]) -> list[Path]:  # noqa: WPS213
        """
        Method for uploading files to remote host.

        Params
        -------
        files_list : List[str | os.PathLike]
            List of files on local storage

        Returns
        -------
        uploaded_files : List[Path]
            List of uploaded files

        Examples
        --------

        Upload files

        .. code::

            uploaded_files = uploader.run(files_list)

        """
        decorated_log(msg="FileUploader starts")

        log.info(f"|Local FS| -> |{self.connection.__class__.__name__}| Uploading files to path: {self.target_path} ")
        log.info(f"|{self.__class__.__name__}| Using params:")
        log.info(" " * LOG_INDENT + f"target_path = {self.target_path}")
        log.info(" " * LOG_INDENT + f"temp_path = {self.temp_path}")

        log.info(f"|{self.__class__.__name__}| Using connection:")
        log.info(" " * LOG_INDENT + f"type = {self.connection.__class__.__name__}")
        log.info(" " * LOG_INDENT + f"host = {self.connection.host}")
        log.info(" " * LOG_INDENT + f"user = {self.connection.user}")

        if not files_list:
            log.warning("|Local| Files list is empty. Please, provide files to upload.")
            return files_list

        if not self.connection.path_exists(self.target_path):
            log.info(f"|{self.connection.__class__.__name__}| There is no target directory: {self.target_path}")
            log.info(f"|{self.connection.__class__.__name__}| Creating directory: {self.target_path}")
            self.connection.mkdir(self.target_path)

        successfully_uploaded_files = []
        files_size = 0
        current_temp_dir = self.temp_path.format(str(uuid.uuid4()))

        if not self.connection.path_exists(current_temp_dir):
            log.info(f"|{self.connection.__class__.__name__}| There is no temp directory: {current_temp_dir}")
            log.info(f"|{self.connection.__class__.__name__}| Creating directory: {current_temp_dir}")
            self.connection.mkdir(current_temp_dir)

        log.info(f"|{self.connection.__class__.__name__}| Start uploading files")

        for count, file_path in enumerate(files_list):
            log.info(f"Uploading {count + 1}/{len(files_list)} ")
            filename = Path(file_path).name
            tmp_file = PosixPath(current_temp_dir) / filename
            target_file = self.target_path / filename
            try:
                self.connection.upload_file(file_path, tmp_file)

                self.connection.rename(tmp_file, target_file)

                file_size = file_path.stat().st_size
            except Exception as e:
                log.exception(
                    f"|{self.connection.__class__.__name__}| Couldn't load file {file_path} "
                    f"to target dir {self.target_path}.\nError:\n{e}",
                )
            else:
                successfully_uploaded_files.append(target_file)
                files_size += file_size
            finally:
                log.info(f"|{self.connection.__class__.__name__}| Removing temp directory: {current_temp_dir}")
                self.connection.rmdir(current_temp_dir, recursive=True)

        log.info(f"|{self.connection.__class__.__name__}| Files successfully uploaded from Local FS")

        msg = f"Uploaded: {len(successfully_uploaded_files)} file(s) {humanize.naturalsize(files_size)}"
        decorated_log(msg=msg, char="-")

        return successfully_uploaded_files
