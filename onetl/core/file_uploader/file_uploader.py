from __future__ import annotations

import os
import uuid
from dataclasses import InitVar, dataclass, field
from logging import getLogger
from pathlib import Path, PurePosixPath

import humanize

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.log import LOG_INDENT, entity_boundary_log

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

    delete_local: bool, default: ``False``
        Delete local files after successful upload.

        .. warning ::
            USE WITH CAUTION BECAUSE FILES WILL BE PERMANENTLY DELETED

    Examples
    --------
    Simple Uploader creation

    .. code::

        from onetl.connection import HDFS
        from onetl.core import FileUploader

        hdfs = HDFS(...)

        uploader = FileUploader(
            connection=hdfs,
            target_path="/path/to/remote/source",
        )

    Uploader with all parameters

    .. code::

        from onetl.connection import HDFS
        from onetl.core import FileUploader

        hdfs = HDFS(...)

        uploader = FileUploader(
            connection=hdfs,
            target_path="/path/to/remote/source",
            temp_path="/home/onetl"
        )

    """

    connection: FileConnection
    target_path: InitVar[str | os.PathLike]
    _target_path: PurePosixPath = field(init=False)
    delete_local: bool = False

    temp_path: InitVar[str | os.PathLike] = field(default="/tmp")
    _temp_path: PurePosixPath = field(init=False)

    def __post_init__(self, target_path: str | os.PathLike, temp_path: str | os.PathLike):
        self._target_path = PurePosixPath(target_path)  # noqa: WPS601
        self._temp_path = PurePosixPath(temp_path)  # noqa: WPS601

    def run(self, files_list: list[str | os.PathLike]) -> list[Path]:  # noqa: WPS213, WPS231
        """
        Method for uploading files to remote host.

        Parameters
        ----------
        files_list : List[str | os.PathLike]
            List of files on local storage

        Returns
        -------
        uploaded_files : List[PurePosixPath]
            List of uploaded files

        Examples
        --------

        Upload files

        .. code::

            uploaded_files = uploader.run(files_list)
        """

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

        if self.delete_local:
            log.warning(" ")
            log.warning(f"|{self.__class__.__name__}| LOCAL FILES WILL BE PERMANENTLY DELETED AFTER UPLOADING !!!")

        if not files_list:
            log.warning(" ")
            log.warning(f"|{self.__class__.__name__}| Files list is empty. Please, provide files to upload.")
            return []

        if not self.connection.path_exists(self._target_path):
            log.info(f"|{connection_class_name}| There is no target directory {self._target_path}, creating ...")
            self.connection.mkdir(self._target_path)

        successfully_uploaded_files = []
        files_size = 0

        current_temp_dir = self._temp_path / uuid.uuid4().hex
        if not self.connection.path_exists(current_temp_dir):
            log.info(f"|{connection_class_name}| Creating temp directory: {current_temp_dir}")
            self.connection.mkdir(current_temp_dir)

        log.info(f"|{self.__class__.__name__}| Start uploading files")
        for count, file in enumerate(files_list):
            log.info(f"|{self.__class__.__name__}| Uploading {count + 1}/{len(files_list)}")

            file_path = Path(file)

            file_name = Path(file).name

            tmp_file = current_temp_dir / file_name
            target_file = self._target_path / file_name

            try:
                file_size = file_path.stat().st_size

                self.connection.upload_file(file_path, tmp_file)
                self.connection.rename(tmp_file, target_file)

                successfully_uploaded_files.append(target_file)
                files_size += file_size

                # Remove files
                if self.delete_local:
                    file_path.unlink()

            except Exception as e:
                log.exception(f"|{self.__class__.__name__}| Couldn't upload file to target dir:")
                log.exception(" " * LOG_INDENT + f"file = {file_path} ")
                log.exception(" " * LOG_INDENT + f"target_file = {target_file}")
                log.exception(" " * LOG_INDENT + f"error = {e}")

        log.info(f"|{connection_class_name}| Removing temp directory: {current_temp_dir}")
        self.connection.rmdir(current_temp_dir, recursive=True)

        log.info(f"|{connection_class_name}| Files successfully uploaded from Local FS")
        msg = f"Uploaded: {len(successfully_uploaded_files)} file(s) {humanize.naturalsize(files_size)}"
        entity_boundary_log(msg=msg, char="-")

        return successfully_uploaded_files
