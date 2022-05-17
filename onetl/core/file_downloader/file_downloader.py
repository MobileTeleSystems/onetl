from __future__ import annotations

import os
import shutil
from dataclasses import InitVar, dataclass, field
from logging import getLogger
from pathlib import Path, PurePosixPath
from typing import Iterable

from onetl.base import BaseFileFilter
from onetl.connection import FileConnection, FileWriteMode
from onetl.core.file_downloader.download_result import DownloadResult
from onetl.core.file_result import FileSet
from onetl.impl import FailedRemoteFile, RemoteFile
from onetl.log import LOG_INDENT, entity_boundary_log, log_with_indent

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

    options : Options | dict | None, default: ``None``
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

    def run(self, files: Iterable[str | os.PathLike] | None = None) -> DownloadResult:  # noqa: WPS231, WPS213 NOSONAR
        """
        Method for downloading files from source to local directory.

        Parameters
        ----------

        files : Iterable[str | os.PathLike] | None, default ``None``
            File collection to download.

            If empty, download files from ``source_path``, applying ``filter`` to each one (if set).

            If not, download input files without any filtering

        Returns
        -------
        downloaded_files : :obj:`onetl.core.file_downloader.download_result.DownloadResult`

            Download result object

        Raises
        -------
        DirectoryNotFoundError

            ``source_path`` does not found

        NotADirectoryError

            ``source_path`` or ``local_path`` is not a directory

        Examples
        --------

        Download files from ``source_path``

        .. code:: python

            from pathlib import Path, PurePath
            from onetl.impl import RemoteFile
            from onetl.core import FileDownloader

            downloader = FileDownloader(source_path="/remote", local_path="/local", ...)
            downloaded_files = downloader.run()

            assert downloaded_files.success == {
                Path("/local/path/file1.txt"),
                Path("/local/path/file2.txt"),
                Path("/local/path/nested/file3.txt"),  # directory structure is preserved
            }
            assert downloaded_files.failed == {FailedRemoteFile("/failed/file")}
            assert downloaded_files.skipped == {RemoteFile("/existing/file")}
            assert downloaded_files.missing == {PurePosixPath("/missing/file")}

        Download only certaing files from ``source_path``

        .. code:: python

            from pathlib import Path, PurePath
            from onetl.impl import RemoteFile
            from onetl.core import FileDownloader

            downloader = FileDownloader(source_path="/remote", local_path="/local", ...)

            downloaded_files = downloader.run(
                [
                    "/remote/path/file1.txt",
                    "/remote/path/nested/file3.txt",
                    # excluding "/remote/path/file2.txt"
                ]
            )

            assert downloaded_files.success == {
                Path("/local/path/file1.txt"),
                Path("/local/path/nested/file3.txt"),  # directory structure is preserved
            }
            assert not downloaded_files.failed
            assert not downloaded_files.skipped
            assert not downloaded_files.missing

        Download certain files from any folder

        .. code:: python

            from pathlib import Path, PurePath
            from onetl.impl import RemoteFile
            from onetl.core import FileDownloader

            downloader = FileDownloader(local_path="/local", ...)  # no source_path set

            downloaded_files = downloader.run(
                [
                    "/remote/path/file1.txt",
                    "/remote/path/nested/file3.txt",
                ]
            )

            assert downloaded_files.success == {
                Path("/local/path/file1.txt"),
                Path("/local/path/file3.txt"),  # directory structure is not preserved
            }
            assert not downloaded_files.failed
            assert not downloaded_files.skipped
            assert not downloaded_files.missing
        """

        if files is None and not self._source_path:
            raise ValueError("Neither file collection nor ``source_path`` are passed")

        # Log all options
        entity_boundary_log(msg="FileDownloader starts")
        indent = len(f"|{self.__class__.__name__}| ") + 2

        log.info(
            f"|{self.connection.__class__.__name__}| -> |Local FS| Downloading files from path '{self._source_path}'"
            f" to local directory: '{self._local_path}'",
        )
        log.info(f"|{self.__class__.__name__}| Using parameters:")
        log.info(" " * indent + f"delete_source = {self._options.delete_source}")

        if self.filter:
            self.filter.log_options()
        else:
            log.info(" " * indent + "filter = None")

        if self._options.delete_source:
            log.warning(f"|{self.__class__.__name__}| SOURCE FILES WILL BE PERMANENTLY DELETED AFTER DOWNLOADING !!!")

        if files and self._source_path:
            log.warning(
                f"|{self.__class__.__name__}| Passed both ``source_path`` and file collection at the same time. "
                "File collection will be used",
            )

        # Check everything
        self._check_local_path()
        self.connection.check()
        log.info("")

        if self._source_path:
            self._check_source_path()

        if files is None:
            log.info(f"|{self.__class__.__name__}| File collection is not passed to `run` method")
            files = self.view_files()

        to_download = self._validate_files(files)

        # TODO:(@dypedchenk) discuss the need for a mode DELETE_ALL
        if self._options.mode == FileWriteMode.DELETE_ALL:
            log.warning(f"|{self.__class__.__name__}| LOCAL DIRECTORY WILL BE CLEANED UP BEFORE DOWNLOADING FILES !!!")
            shutil.rmtree(self._local_path)
            self._local_path.mkdir()

        log.info(f"|{self.__class__.__name__}| Starting downloading files")

        result = DownloadResult()
        total_files = len(to_download)

        for i, (source_file, local_file) in enumerate(to_download):
            log.info(f"|{self.__class__.__name__}| Uploading file {i+1} of {total_files}")
            log.info(" " * LOG_INDENT + f"from = '{source_file}'")
            log.info(" " * LOG_INDENT + f"to = '{local_file}'")

            if not self.connection.path_exists(source_file):
                log.warning(f"|{self.__class__.__name__}| Missing file '{source_file}', skipping")
                result.missing.add(source_file)
                continue

            try:
                remote_file = self.connection.get_file(source_file)

                replace = False
                if local_file.exists():
                    error_message = f"Local directory already contains file '{local_file}'"
                    if self._options.mode == FileWriteMode.ERROR:
                        raise FileExistsError(error_message)

                    if self._options.mode == FileWriteMode.IGNORE:
                        log.warning(f"|LocalFS| {error_message}, skipping")
                        result.skipped.add(remote_file)
                        continue

                    replace = True
                    log.warning(f"|LocalFS| {error_message}, overwriting")

                # Download
                self.connection.download_file(remote_file, local_file, replace=replace)

                # Delete Remote
                if self._options.delete_source:
                    self.connection.remove_file(remote_file)

                result.success.add(local_file)

            except Exception as e:
                log.exception(
                    f"|{self.__class__.__name__}| Couldn't download file from target dir: {e}",
                    exc_info=False,
                )
                result.failed.add(FailedRemoteFile(path=remote_file.path, stats=remote_file.stats, exception=e))

        log.info(f"|{self.__class__.__name__}| Download result:")
        log_with_indent(str(result))
        entity_boundary_log(msg=f"{self.__class__.__name__} ends", char="-")

        return result

    def view_files(self) -> FileSet[RemoteFile]:
        """
        Get file collection in the ``source_path``, after ``filter`` applied (if any)

        Raises
        -------
        DirectoryNotFoundError

            ``source_path`` does not found

        NotADirectoryError

            ``source_path`` is not a directory

        ValueError

            File in ``files`` argument does not match ``source_path``

        Returns
        -------
        FileSet[RemoteFile]
            Set of files in ``source_path``

        Examples
        --------

        View files

        .. code:: python

            from onetl.impl import RemoteFile
            from onetl.core import FileDownloader

            downloader = FileDownloader(source_path="/remote/path", ...)

            view_files = downloader.view_files()

            assert view_files == {
                RemoteFile("/remote/path/file1.txt"),
                RemoteFile("/remote/path/file3.txt"),
                RemoteFile("/remote/path/nested/file3.txt"),
            }
        """

        log.info(f"|{self.connection.__class__.__name__}| Getting files list from path: '{self._source_path}'")

        self._check_source_path()
        result = FileSet()

        try:
            for root, dirs, files in self.connection.walk(self._source_path, filter=self.filter):
                log.debug(
                    f"|{self.connection.__class__.__name__}| "
                    f"Listing dir '{root}', dirs: {len(dirs)} files: {len(files)}",
                )

                result.update(RemoteFile(path=root / file, stats=file.stats) for file in files)

        except Exception as e:
            raise RuntimeError(
                f"Couldn't read directory tree from remote dir {self._source_path}",
            ) from e

        return result

    def _validate_files(self, remote_files: Iterable[os.PathLike | str]) -> list[tuple[PurePosixPath, Path]]:
        result = []

        for remote_file in remote_files:
            remote_file_path = PurePosixPath(remote_file)

            if not self._source_path:
                # Download into a flat structure
                if not remote_file_path.is_absolute():
                    raise ValueError("Cannot pass relative file path with empty ``source_path``")

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
                    raise ValueError(f"File path '{remote_file_path}' does not match source_path '{self._source_path}'")

            result.append((remote_file_path, local_file_path))

        return result

    def _check_source_path(self):
        if not self.connection.is_dir(self._source_path):
            raise NotADirectoryError(f"'{self._source_path}' is not a directory")

    def _check_local_path(self):
        if self._local_path.exists() and not self._local_path.is_dir():
            raise NotADirectoryError(f"'{self._local_path}' is not a directory")

        self._local_path.mkdir(exist_ok=True, parents=True)
