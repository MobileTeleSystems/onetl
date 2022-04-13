from __future__ import annotations

from dataclasses import dataclass, field
from logging import getLogger
from pathlib import Path, PosixPath, PurePosixPath
from typing import Iterator
import shutil

import humanize

from onetl.connection.file_connection.file_connection import FileConnection
from onetl.core.file_downloader.downloader_helper import check_pattern, create_local_dir
from onetl.connection.file_connection.file_connection import WriteMode
from onetl.log import entity_boundary_log

log = getLogger(__name__)


@dataclass
# TODO:(@mivasil6) make check_history functional
class FileDownloader:
    """Class specifies file source where you can download files. Download files **only** in local directory.

    Parameters
    ----------
    connection : :obj:`onetl.connection.FileConnection`
        Class which contains File system connection properties. See in FileConnection section.

    source_path : str
        Path on remote source where you got files.

    local_path : str
        Local path where you download files

    file_pattern : str, default: ``*``
        Fnmatch check for file_name. For example: ``*.csv``.

    source_exclude_dirs : list of str, default: ``None``
        A list of dirs excluded from loading. Must contain full path to excluded dir.

    options: Options | dict | None, default: ``None``
        File downloading options

    Examples
    --------
    Simple Downloader creation

    .. code::

        from onetl.connection import SFTP
        from onetl.core import FileDownloader

        sftp = SFTP(...)

        downloader = FileDownloader(
            connection=sftp,
            source_path="/path/to/remote/source",
            local_path="/path/to/local",
        )

    Downloader with all parameters

    .. code::

        from onetl.connection import SFTP
        from onetl.core import FileDownloader

        sftp = SFTP(...)

        downloader = FileDownloader(
            connection=sftp,
            source_path="/path/to/remote/source",
            local_path="/path/to/local",
            source_exclude_dirs=["path/to/remote/source/exclude_dir"],
            file_pattern="*.txt",
        )
    """

    connection: FileConnection
    source_path: Path | str
    local_path: Path | str
    options: FileConnection.Options | dict | None = None
    file_pattern: str | None = "*"
    source_exclude_dirs: list = field(default_factory=list)
    _options: FileConnection.Options = field(init=False)

    def __post_init__(self):
        self.source_path = PosixPath(self.source_path)
        self.local_path = Path(self.local_path)

        self._options = self.options or self.connection.Options()  # noqa: WPS601

        if isinstance(self._options, dict):
            self._options = self.connection.Options.parse_obj(self.options)  # noqa: WPS601

    def remote_files_listing(self, source_path: Path | str) -> Iterator:
        log.info(f"|{self.connection.__class__.__name__}| Getting files list from path: {source_path}")

        try:
            ftp_walk = self.connection.walk(
                source_path,
                topdown=True,
                onerror=log.exception,
                exclude_dirs=self.source_exclude_dirs,
            )
        except Exception as e:
            raise RuntimeError(
                f"Couldn`t read directory tree from remote dir {source_path}.\n Error message:\n {e}",
            )

        for root, dirs, files in ftp_walk:
            log.debug(
                f"|{self.connection.__class__.__name__}| "
                f'Listing dir f"{root}", dirs: {len(dirs)} files: {len(files)}',
            )
            for res_file in files:
                log.info(f"|{self.connection.__class__.__name__}| Checking file: {PosixPath(root) / res_file}")
                try:
                    check_pattern(res_file, self.file_pattern)
                except Exception as e:
                    log.info(e)
                    continue

                file_path = PosixPath(root) / res_file
                log.info(f"Add file to batch: {file_path}")
                yield file_path

    def run(self) -> list[Path]:  # noqa: WPS231, WPS213
        """
        Method for downloading files from source to local directory.

        Returns
        -------
        downloaded_files : List[str]
            List of downloaded files

        Examples
        --------

        Download files

        .. code::

            downloaded_files = downloader.run()

        """
        entity_boundary_log(msg="FileDownloader starts")
        indent = len(f"|{self.__class__.__name__}| ") + 2

        log.info(
            f"|{self.connection.__class__.__name__}| -> |Local FS| Downloading files from path '{self.source_path}'"
            f" to local directory: '{self.local_path}'",
        )
        log.info(f"|{self.__class__.__name__}| Using parameters:")
        log.info(" " * indent + f"file_pattern = {self.file_pattern}")
        log.info(" " * indent + f"delete_source = {self._options.delete_source}")
        log.info(" " * indent + f"source_exclude_dirs = {self.source_exclude_dirs}")
        log.info(f"|{self.__class__.__name__}| Using connection:")
        log.info(" " * indent + f"type = {self.connection.__class__.__name__}")
        log.info(" " * indent + f"host = {self.connection.host}")
        log.info(" " * indent + f"user = {self.connection.user}")

        if self._options.delete_source:
            log.warning(" ")
            log.warning(f"|{self.__class__.__name__}| SOURCE FILES WILL BE PERMANENTLY DELETED AFTER DOWNLOADING !!!")

        downloaded_files = []
        downloaded_remote_files = []
        files_size = 0
        last_exception = None

        # TODO:(@mivasil6) не выводить лог, если папка есть
        create_local_dir(self.local_path)

        # TODO:(@dypedchenk) discuss the need for a mode DELETE_ALL
        if self._options.mode == WriteMode.DELETE_ALL:
            shutil.rmtree(self.local_path)
            self.local_path.mkdir()

        for remote_file_path in self.view_files():
            filename = remote_file_path.name
            local_file_path = Path(self.local_path) / filename

            try:
                if local_file_path.exists():
                    error_message = (
                        f"|{self.__class__.__name__}| Target directory already contains file '" f"{local_file_path}'"
                    )
                    if self._options.mode == WriteMode.ERROR:
                        raise RuntimeError(error_message)  # noqa: WPS220

                    if self._options.mode == WriteMode.IGNORE:
                        log.warning(error_message + ", skipping")  # noqa: WPS220
                        continue  # noqa: WPS220

                    if self._options.mode == WriteMode.OVERWRITE:
                        log.warning(error_message + ", overwriting")  # noqa: WPS220
                        local_file_path.unlink()  # noqa: WPS220

                # Download
                self.connection.download_file(remote_file_path, local_file_path)

                # Delete Remote
                if self._options.delete_source:
                    self.connection.remove_file(remote_file_path)

                file_size = local_file_path.stat().st_size

            except Exception as e:
                last_exception = e
                log.error(
                    f"|{self.connection.__class__.__name__}| Download file {remote_file_path} "
                    f"from remote to {self.local_path} failed with:\n{last_exception}",
                )
            else:
                downloaded_files.append(local_file_path)
                downloaded_remote_files.append(remote_file_path)
                files_size += file_size

        if not downloaded_files and not last_exception:
            log.warning("There are no files on remote server")
        elif last_exception:
            log.error("There are some errors with files. Check previous logs.")
            raise last_exception
        else:
            log.info(f"|Local FS| Files successfully downloaded from {self.connection.__class__.__name__}")

        msg = f"Downloaded: {len(downloaded_files)} file(s) {humanize.naturalsize(files_size)}"
        entity_boundary_log(msg=msg, char="-")

        return downloaded_files

    def view_files(self) -> list[PurePosixPath]:
        """
        Method to show list of downloaded files from source

        Returns
        -------
        List[Path]
            List of downloaded files.

        Examples
        --------

        View files

        .. code::

            view_files = downloader.view_files()

        """

        return list(self.remote_files_listing(self.source_path))
