from __future__ import annotations

import os
import shutil
from dataclasses import InitVar, dataclass, field
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterator

import humanize

from onetl.base import BaseFileFilter
from onetl.connection import FileConnection, FileWriteMode
from onetl.impl import RemoteFile
from onetl.log import entity_boundary_log

log = getLogger(__name__)


@dataclass
# TODO:(@mivasil6) make check_history functional
class FileDownloader:
    """Class specifies file source where you can download files. Download files **only** to local directory.

    Parameters
    ----------
    connection : :obj:`onetl.connection.FileConnection`
        Class which contains File system connection properties. See in FileConnection section.

    source_path : os.PathLike | str | None, default: ``None``
        Path on remote source to download files from.

        Could be ``None``, but only if you pass file paths directly to
        :obj:`onetl.core.file_downloader.file_downloader.FileDownloader.run` method

    local_path : os.PathLike | str
        Local path where you download files

    filter : BaseFileFilter
        Options of the file filtering. See :obj:`onetl.core.file_filter.file_filter.FileFilter`

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
            filter=FileFilter(glob="*.txt", exclude_dirs=["/path/to/remote/source/exclude_dir"]),
        )
    """

    connection: FileConnection
    local_path: InitVar[os.PathLike | str]
    _local_path: Path = field(init=False)

    source_path: InitVar[os.PathLike | str | None] = field(default=None)
    _source_path: PurePosixPath | None = field(init=False)

    filter: BaseFileFilter | None = None

    options: InitVar[FileConnection.Options | dict | None] = field(default=None)
    _options: FileConnection.Options = field(init=False)

    def __post_init__(
        self,
        local_path: os.PathLike | str,
        source_path: os.PathLike | str | None,
        options: FileConnection.Options | dict | None,
    ):
        self._local_path = Path(local_path).resolve()
        self._source_path = PurePosixPath(source_path) if source_path else None

        if isinstance(options, dict):
            self._options = self.connection.Options.parse_obj(options)
        else:
            self._options = options or self.connection.Options()

    def run(self, files_list: list[str | os.PathLike] | None = None) -> list[Path]:  # noqa: WPS231, WPS213 NOSONAR
        """
        Method for downloading files from source to local directory.

        Parameters
        ----------

        files_list : list[str | os.PathLike] | None, default ``None``
            List files to download.

            If empty, download files from ``source_path``, applying ``filter`` to each one (if set).

            If not, download input files without any filtering

        Returns
        -------
        downloaded_files : List[Path]
            List of downloaded files

        Raises
        -------
        Exception
            Errors during data loading

        Examples
        --------

        Download files from ``source_path``

        .. code:: python

            from pathlib import Path
            from onetl.core import FileDownloader

            downloader = FileDownloader(source_path="/remote/path", local_path="/local/path")

            downloaded_files = downloader.run()

            assert downloaded_files == [
                Path("/local/path/file1.txt"),
                Path("/local/path/file2.txt"),
                Path("/local/path/nested/file3.txt"),  # directory structure is preserved
            ]

        Download only certaing files from ``source_path``

        .. code::

            from pathlib import Path
            from onetl.core import FileDownloader

            downloader = FileDownloader(source_path="/remote/path", local_path="/local/path")

            downloaded_files = downloader.run([
                "/remote/path/file1.txt",
                "/remote/path/nested/file3.txt",
            ])

            # relative paths can be used too
            downloaded_files = downloader.run([
                "file1.txt",
                "nested/file3.txt",
            ])

            assert downloaded_files == == [
                Path("/local/path/file1.txt"),
                Path("/local/path/nested/file3.txt"),  # directory structure is preserved
            ]

        Download certain files from any folder

        .. code::

            from pathlib import Path
            from onetl.core import FileDownloader

            # no ``source_path``
            downloader = FileDownloader(local_path="/local/path")

            # only absolute paths in this case
            downloaded_files = downloader.run([
                "/remote/path/file1.txt",
                "/remote/path/nested/file3.txt",
            ])

            assert downloaded_files == == [
                Path("/local/path/file1.txt"),
                Path("/local/path/file3.txt"),  # directory structure is not preserved
            ]

        """

        if files_list is None and not self._source_path:
            raise ValueError(f"|{self.__class__.__name__}| No list of files to upload nor ``source_path`` passed")

        entity_boundary_log(msg="FileDownloader starts")
        indent = len(f"|{self.__class__.__name__}| ") + 2

        log.info(
            f"|{self.connection.__class__.__name__}| -> |Local FS| Downloading files from path '{self._source_path}'"
            f" to local directory: '{self._local_path}'",
        )
        log.info(f"|{self.__class__.__name__}| Using parameters:")

        if self.filter:
            self.filter.log_options()

        log.info(" " * indent + f"delete_source = {self._options.delete_source}")
        log.info(f"|{self.__class__.__name__}| Using connection:")
        log.info(" " * indent + f"type = {self.connection.__class__.__name__}")
        log.info(" " * indent + f"host = {self.connection.host}")
        log.info(" " * indent + f"user = {self.connection.user}")

        self.connection.check()

        if self._options.delete_source:
            log.warning(" ")
            log.warning(f"|{self.__class__.__name__}| SOURCE FILES WILL BE PERMANENTLY DELETED AFTER DOWNLOADING !!!")

        downloaded_files = []
        downloaded_remote_files = []
        files_size = 0
        last_exception = None

        # TODO:(@dypedchenk) discuss the need for a mode DELETE_ALL
        if self._options.mode == FileWriteMode.DELETE_ALL:
            shutil.rmtree(self._local_path)

        self._local_path.mkdir(exist_ok=True, parents=True)

        if files_list is None:
            files_list = self.view_files()

        for remote_file, local_file in self._validate_files_list(files_list):
            try:
                replace = False
                if local_file.exists():
                    error_message = f"|{self.__class__.__name__}| Target directory already contains file '{local_file}'"
                    if self._options.mode == FileWriteMode.ERROR:
                        raise FileExistsError(error_message)

                    if self._options.mode == FileWriteMode.IGNORE:
                        log.warning(error_message + ", skipping")
                        continue

                    replace = True
                    log.warning(error_message + ", overwriting")

                # Download
                self.connection.download_file(remote_file, local_file, replace=replace)

                # Delete Remote
                if self._options.delete_source:
                    self.connection.remove_file(remote_file)

                file_size = local_file.stat().st_size

            except Exception as e:
                last_exception = e
                log.error(
                    f"|{self.connection.__class__.__name__}| Download file {remote_file} "
                    f"from remote to {self._local_path} failed with:\n{last_exception}",
                )
            else:
                downloaded_files.append(local_file)
                downloaded_remote_files.append(remote_file)
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

    def view_files(self) -> list[RemoteFile]:
        """
        Show list of files in the source, after ``filter`` applied

        Returns
        -------
        List[Path]
            List of downloaded files.

        Examples
        --------

        View files

        .. code:: python

            from onetl.impl import RemoteFile
            from onetl.core import FileDownloader

            downloader = FileDownloader(source_path="/remote/path", local_path="/local/path")

            view_files = downloader.view_files()

            assert view_files == [
                RemoteFile("/remote/path/file1.txt"),
                RemoteFile("/remote/path/file3.txt"),
                RemoteFile("/remote/path/nested/file3.txt"),
            ]
        """

        return list(self._remote_files_listing(self._source_path))

    def _remote_files_listing(self, source_path: os.PathLike | str) -> Iterator[RemoteFile]:
        log.info(f"|{self.connection.__class__.__name__}| Getting files list from path: '{source_path}'")

        try:
            for root, dirs, files in self.connection.walk(source_path, filter=self.filter):
                log.debug(
                    f"|{self.connection.__class__.__name__}| "
                    f"Listing dir '{root}', dirs: {len(dirs)} files: {len(files)}",
                )

                for file in files:  # noqa: WPS526
                    yield RemoteFile(path=root / file, stats=file.stats)

        except Exception as e:
            raise RuntimeError(
                f"Couldn't read directory tree from remote dir {source_path}",
            ) from e

    def _validate_files_list(self, remote_files: list[os.PathLike | str]) -> list[tuple[PurePosixPath, Path]]:
        result = []

        for remote_file in remote_files:
            remote_file_path = PurePosixPath(remote_file)

            if not self._source_path:
                # Download into a flat structure
                if not remote_file_path.is_absolute():
                    raise ValueError(
                        f"|{self.__class__.__name__}| Cannot pass relative file path with empty ``source_path``",
                    )

                filename = remote_file_path.name
                local_file_path = self._local_path / filename
            else:
                # Download according to source folder structure
                if self._source_path in remote_file_path.parents:
                    # Make relative local path
                    local_file_path = self._local_path / remote_file_path.relative_to(self._source_path)

                elif not remote_file_path.is_absolute():
                    # Passed already relative path
                    local_file_path = self._local_path / remote_file_path
                    remote_file_path = self._source_path / remote_file_path
                else:
                    # Wrong path (not relative path and source path not in the path to the file)
                    raise ValueError(
                        f"|{self.__class__.__name__}| File path '{remote_file_path}' "
                        f"does not match source_path '{self._source_path}'",
                    )

            result.append((remote_file_path, local_file_path))

        return result
