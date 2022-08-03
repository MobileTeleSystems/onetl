from __future__ import annotations

import os
import shutil
from dataclasses import InitVar, dataclass, field
from logging import getLogger
from typing import Iterable, Tuple

from etl_entities import FileListHWM, RemoteFolder
from ordered_set import OrderedSet
from pydantic import BaseModel

from onetl.base import BaseFileFilter, BaseFileLimit
from onetl.connection import FileConnection
from onetl.core.file_downloader.download_result import DownloadResult
from onetl.core.file_limit import FileLimit
from onetl.core.file_result import FileSet
from onetl.exception import NotAFileError
from onetl.impl import (
    FailedRemoteFile,
    FileWriteMode,
    LocalPath,
    RemoteFile,
    RemotePath,
)
from onetl.log import LOG_INDENT, entity_boundary_log, log_with_indent
from onetl.strategy import BaseStrategy, StrategyManager
from onetl.strategy.batch_hwm_strategy import BatchHWMStrategy
from onetl.strategy.hwm_store import HWMStoreManager
from onetl.strategy.hwm_strategy import HWMStrategy

log = getLogger(__name__)

DOWNLOAD_ITEMS_TYPE = OrderedSet[Tuple[RemotePath, LocalPath]]


@dataclass
# TODO:(@mivasil6) make check_history functional
class FileDownloader:
    """Class specifies file source where you can download files. Download files **only** to local directory.

    Parameters
    ----------
    connection : :obj:`onetl.connection.FileConnection`
        Class which contains File system connection properties. See in FileConnection section.

    source_path : os.PathLike | str | None, default: ``None``
        Remote path to download files from.

        Could be ``None``, but only if you pass file paths directly to
        :obj:`onetl.core.file_downloader.file_downloader.FileDownloader.run` method

    local_path : os.PathLike | str
        Local path where you download files

    filter : BaseFileFilter
        Options of the file filtering. See :obj:`onetl.core.file_filter.file_filter.FileFilter`

    limit : BaseFileLimit
        Options of the file  limiting. See :obj:`onetl.core.file_limit.file_limit.FileLimit`
        Default file count limit is 100

    options : :obj:`onetl.core.file_downloader.file_downloader.FileDownloader.Options`  | dict | None, default: ``None``
        File downloading options

    hwm_type : str | None, default: ``None``
        hwm type to be used.

        .. warning ::
            Used only in incremental strategy.

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
        from onetl.core import FileDownloader, FileFilter, FileLimit

        sftp = SFTP(...)

        downloader = FileDownloader(
            connection=sftp,
            source_path="/path/to/remote/source",
            local_path="/path/to/local",
            filter=FileFilter(glob="*.txt", exclude_dirs=["/path/to/remote/source/exclude_dir"]),
            options=FileDownloader.Options(delete_source=True, mode="overwrite"),
            limit=FileLimit(count_limit=10),
        )

    Incremental loading:

    .. code::

        from onetl.connection import SFTP
        from onetl.core import FileDownloader, FileFilter, FileLimit
        from onetl.strategy import IncrementalStrategy

        sftp = SFTP(...)

        downloader = FileDownloader(
            connection=sftp,
            source_path="/path/to/remote/source",
            local_path="/path/to/local",
            filter=FileFilter(glob="*.txt", exclude_dirs=["/path/to/remote/source/exclude_dir"]),
            options=FileDownloader.Options(delete_source=True, mode="overwrite"),
            limit=FileLimit(count_limit=10),
            hwm_type="file_list",
        )

        with IncrementalStrategy():
            downloader.run()

    """

    class Options(BaseModel):  # noqa: WPS431
        """File downloader options"""

        mode: FileWriteMode = FileWriteMode.ERROR
        """
        How to handle existing files in the local directory.

        Possible values:
            * ``error`` (default) - do nothing, mark file as failed
            * ``ignore`` - do nothing, mark file as ignored
            * ``overwrite`` - replace existing file with a new one
            * ``delete_all`` - delete local directory content before downloading files
        """

        delete_source: bool = False
        """
        If ``True``, remove source file after successful download.

        If download failed, file will left intact.
        """

        class Config:  # noqa: WPS431
            frozen = True

    connection: FileConnection

    local_path: InitVar[os.PathLike | str]
    _local_path: LocalPath = field(init=False)

    filter: BaseFileFilter | None = None

    limit: BaseFileLimit = FileLimit()

    options: InitVar[Options | dict | None] = field(default=None)
    _options: Options = field(init=False)

    source_path: InitVar[os.PathLike | str | None] = field(default=None)
    _source_path: RemotePath | None = field(init=False)

    hwm_type: str | None = None

    def __post_init__(
        self,
        local_path: os.PathLike | str,
        options: FileConnection.Options | dict | None,
        source_path: os.PathLike | str | None,
    ):
        self._local_path = LocalPath(local_path).resolve()
        self._source_path = RemotePath(source_path) if source_path else None

        if isinstance(options, dict):
            self._options = self.Options.parse_obj(options)
        else:
            self._options = options or self.Options()

    def run(self, files: Iterable[str | os.PathLike] | None = None) -> DownloadResult:  # noqa: WPS231
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

            from onetl.impl import RemoteFile, LocalPath
            from onetl.core import FileDownloader

            downloader = FileDownloader(source_path="/remote", local_path="/local", ...)
            downloaded_files = downloader.run()

            assert downloaded_files.successful == {
                LocalPath("/local/path/file1.txt"),
                LocalPath("/local/path/file2.txt"),
                LocalPath("/local/path/nested/file3.txt"),  # directory structure is preserved
            }
            assert downloaded_files.failed == {FailedRemoteFile("/failed/file")}
            assert downloaded_files.skipped == {RemoteFile("/existing/file")}
            assert downloaded_files.missing == {RemotePath("/missing/file")}

        Download only certaing files from ``source_path``

        .. code:: python

            from onetl.impl import RemoteFile, LocalPath
            from onetl.core import FileDownloader

            downloader = FileDownloader(source_path="/remote", local_path="/local", ...)

            downloaded_files = downloader.run(
                [
                    "/remote/path/file1.txt",
                    "/remote/path/nested/file3.txt",
                    # excluding "/remote/path/file2.txt"
                ]
            )

            assert downloaded_files.successful == {
                LocalPath("/local/path/file1.txt"),
                LocalPath("/local/path/nested/file3.txt"),  # directory structure is preserved
            }
            assert not downloaded_files.failed
            assert not downloaded_files.skipped
            assert not downloaded_files.missing

        Download certain files from any folder

        .. code:: python

            from onetl.impl import RemoteFile, LocalPath
            from onetl.core import FileDownloader

            downloader = FileDownloader(local_path="/local", ...)  # no source_path set

            downloaded_files = downloader.run(
                [
                    "/remote/path/file1.txt",
                    "/remote/path/nested/file3.txt",
                ]
            )

            assert downloaded_files.successful == {
                LocalPath("/local/path/file1.txt"),
                LocalPath("/local/path/file3.txt"),  # directory structure is not preserved
            }
            assert not downloaded_files.failed
            assert not downloaded_files.skipped
            assert not downloaded_files.missing
        """

        self._check_strategy()

        if files is None and not self._source_path:
            raise ValueError("Neither file collection nor ``source_path`` are passed")

        self._log_options(files)

        # Check everything
        self._check_local_path()
        self.connection.check()
        log.info("")

        if self._source_path:
            self._check_source_path()

        if files is None:
            files = self.view_files()
        to_download = self._validate_files(files)

        # remove folder only after everything is checked
        if self._options.mode == FileWriteMode.DELETE_ALL:
            shutil.rmtree(self._local_path)
            self._local_path.mkdir()

        if self.hwm_type is not None:
            result = self._hwm_processing(to_download)
        else:
            result = self._download_files(to_download)

        self._log_result(result)
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
        self.limit.reset_state()

        try:
            for root, dirs, files in self.connection.walk(self._source_path, filter=self.filter):
                # when iterating over files, it should be checked for a limit on the number of files.
                # when the limit on the number of files is reached,
                # it is necessary to stop and download files that have not yet been uploaded.

                log.debug(
                    f"|{self.connection.__class__.__name__}| "
                    f"Listing dir '{root}', dirs: {len(dirs)} files: {len(files)}",
                )

                file_list = []

                for file in files:
                    file_list.append(RemoteFile(path=root / file, stats=file.stats))
                    if self.limit.verify():
                        break  # noqa: WPS220

                if file_list:
                    result.update(file_list)

                if self.limit.is_reached:
                    log.warning("File limit reached !")
                    break

        except Exception as e:
            raise RuntimeError(
                f"Couldn't read directory tree from remote dir {self._source_path}",
            ) from e

        return result

    def _check_strategy(self):
        strategy: BaseStrategy = StrategyManager.get_current()

        if self.hwm_type:
            if not isinstance(strategy, HWMStrategy):
                raise ValueError(f"|{self.__class__.__name__}| `hwm_type` cannot be used in snapshot strategy.")
            elif getattr(strategy, "offset", None):  # this check should be somewhere in IncrementalStrategy,
                # but the logic is quite messy
                raise ValueError(f"|{self.__class__.__name__}| If `hwm_type` is passed you can't specify an `offset`")

            if isinstance(strategy, BatchHWMStrategy):
                raise ValueError(f"|{self.__class__.__name__}| `hwm_type` cannot be used in batch strategy.")

            if not self._source_path:
                raise ValueError(
                    f"|{self.__class__.__name__}| If `hwm_type` is passed, `source_path` must be specified",
                )

    def _hwm_processing(self, to_download: DOWNLOAD_ITEMS_TYPE) -> DownloadResult:
        remote_file_folder = RemoteFolder(name=self._source_path, instance=self.connection.instance_url)
        file_hwm = FileListHWM(source=remote_file_folder)
        file_hwm_name = file_hwm.qualified_name
        current_hwm_store = HWMStoreManager.get_current()
        file_hwm_stored = current_hwm_store.get(file_hwm_name)

        if file_hwm_stored:
            # Exclude files already downloaded before
            already_downloaded = abs(file_hwm_stored)
            to_download = OrderedSet(
                filter(lambda path: path[0] not in already_downloaded, to_download),
            )

        return self._download_files(
            to_download,
            current_hwm_store=current_hwm_store,
            file_hwm_name=file_hwm_name,
            remote_file_folder=remote_file_folder,
        )

    def _log_options(self, files: Iterable[str | os.PathLike] | None = None) -> None:
        entity_boundary_log(msg="FileDownloader starts")

        log.info(f"|{self.connection.__class__.__name__}| -> |Local FS| Downloading files using parameters:")
        log.info(LOG_INDENT + f"source_path = {self._source_path}")
        log.info(LOG_INDENT + f"local_path = {self._local_path}")

        if self.filter is not None:
            log.info("")
            self.filter.log_options()
        else:
            log.info(LOG_INDENT + "filter = None")

        self.limit.log_options()

        log.info(LOG_INDENT + "options:")
        for option, value in self._options.dict().items():
            log.info(LOG_INDENT + f"    {option} = {value}")
        log.info("")

        if self._options.delete_source:
            log.warning(f"|{self.__class__.__name__}| SOURCE FILES WILL BE PERMANENTLY DELETED AFTER DOWNLOADING !!!")

        if self._options.mode == FileWriteMode.DELETE_ALL:
            log.warning(f"|{self.__class__.__name__}| LOCAL DIRECTORY WILL BE CLEANED UP BEFORE DOWNLOADING FILES !!!")

        if files and self._source_path:
            log.warning(
                f"|{self.__class__.__name__}| Passed both ``source_path`` and file collection at the same time. "
                "File collection will be used",
            )

        if not files:
            log.info(f"|{self.__class__.__name__}| File collection is not passed to `run` method")

    def _validate_files(  # noqa: WPS231
        self,
        remote_files: Iterable[os.PathLike | str],
    ) -> DOWNLOAD_ITEMS_TYPE:
        result = OrderedSet()

        for remote_file in remote_files:
            remote_file_path = RemotePath(remote_file)

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

            if self.connection.path_exists(remote_file_path) and not self.connection.is_file(remote_file_path):
                raise NotAFileError(f"|{self.connection.__class__.__name__}| '{remote_file_path}' is not a file")

            result.add((remote_file_path, local_file_path))

        return result

    def _check_source_path(self):
        if not self.connection.is_dir(self._source_path):
            raise NotADirectoryError(f"|{self.connection.__class__.__name__}| '{self._source_path}' is not a directory")

    def _check_local_path(self):
        if self._local_path.exists() and not self._local_path.is_dir():
            raise NotADirectoryError(f"|Local FS| '{self._local_path}' is not a directory")

        self._local_path.mkdir(exist_ok=True, parents=True)

    def _download_files(
        self,
        to_download: DOWNLOAD_ITEMS_TYPE,
        current_hwm_store: HWMStoreManager | None = None,
        file_hwm_name: str | None = None,
        remote_file_folder: RemoteFolder | None = None,
    ) -> DownloadResult:
        total_files = len(to_download)
        files = FileSet(item[0] for item in to_download)

        log.info(f"|{self.__class__.__name__}| Files to be downloaded:")
        log_with_indent(str(files))
        log.info(f"|{self.__class__.__name__}| Starting the download process")

        result = DownloadResult()
        for i, (source_file, local_file) in enumerate(to_download):
            log.info(f"|{self.__class__.__name__}| Uploading file {i+1} of {total_files}")
            log.info(LOG_INDENT + f"from = {source_file}")
            log.info(LOG_INDENT + f"to = {local_file}")

            self._download_file(
                source_file,
                local_file,
                result,
                file_hwm_name=file_hwm_name,
                current_hwm_store=current_hwm_store,
                remote_file_folder=remote_file_folder,
            )

        return result

    def _download_file(  # noqa: WPS231
        self,
        source_file: RemotePath,
        local_file: LocalPath,
        result: DownloadResult,
        current_hwm_store: HWMStoreManager | None = None,
        file_hwm_name: str | None = None,
        remote_file_folder: RemoteFolder | None = None,
    ) -> None:
        if not self.connection.path_exists(source_file):
            log.warning(f"|{self.__class__.__name__}| Missing file '{source_file}', skipping")
            result.missing.add(source_file)
            return

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
                    return

                replace = True
                log.warning(f"|LocalFS| {error_message}, overwriting")

            # Download
            self.connection.download_file(remote_file, local_file, replace=replace)

            # Delete Remote
            if self._options.delete_source:
                self.connection.remove_file(remote_file)

            if current_hwm_store:
                file_hwm_stored = current_hwm_store.get(file_hwm_name) or FileListHWM(source=remote_file_folder)
                file_hwm_stored += local_file.relative_to(self._local_path)
                current_hwm_store.save(file_hwm_stored)

            result.successful.add(local_file)

        except Exception as e:
            log.exception(
                f"|{self.__class__.__name__}| Couldn't download file from target dir: {e}",
                exc_info=False,
            )
            result.failed.add(FailedRemoteFile(path=remote_file.path, stats=remote_file.stats, exception=e))

    def _log_result(self, result: DownloadResult) -> None:
        log.info(f"|{self.__class__.__name__}| Download result:")
        log_with_indent(str(result))
        entity_boundary_log(msg=f"{self.__class__.__name__} ends", char="-")
