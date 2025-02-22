# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import os
import shutil
import textwrap
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from typing import Generator, Iterable, List, Optional, Tuple, Type, Union

from etl_entities.hwm import FileHWM, FileListHWM
from etl_entities.instance import AbsolutePath
from etl_entities.old_hwm import FileListHWM as OldFileListHWM
from etl_entities.source import RemoteFolder
from ordered_set import OrderedSet

try:
    from pydantic.v1 import Field, PrivateAttr, root_validator, validator
except (ImportError, AttributeError):
    from pydantic import Field, PrivateAttr, root_validator, validator  # type: ignore[no-redef, assignment]

from onetl._util.file import generate_temp_path
from onetl.base import BaseFileConnection, BaseFileFilter, BaseFileLimit
from onetl.base.path_protocol import PathProtocol
from onetl.file.file_downloader.options import FileDownloaderOptions
from onetl.file.file_downloader.result import DownloadResult
from onetl.file.file_set import FileSet
from onetl.file.filter.file_hwm import FileHWMFilter
from onetl.hooks import slot, support_hooks
from onetl.impl import (
    FailedRemoteFile,
    FileExistBehavior,
    FrozenModel,
    LocalPath,
    RemoteFile,
    RemotePath,
    path_repr,
)
from onetl.log import (
    entity_boundary_log,
    log_collection,
    log_hwm,
    log_lines,
    log_options,
    log_with_indent,
)
from onetl.strategy import StrategyManager
from onetl.strategy.batch_hwm_strategy import BatchHWMStrategy
from onetl.strategy.hwm_strategy import HWMStrategy

log = logging.getLogger(__name__)

# source, target, temp
DOWNLOAD_ITEMS_TYPE = OrderedSet[Tuple[RemotePath, LocalPath, Optional[LocalPath]]]


class FileDownloadStatus(Enum):
    SUCCESSFUL = 0
    FAILED = 1
    SKIPPED = 2
    MISSING = -1


@support_hooks
class FileDownloader(FrozenModel):
    """Allows you to download files from a remote source with specified file connection
    and parameters, and return an object with download result summary. |support_hooks|

    .. note::

        FileDownloader can return different results depending on :ref:`strategy`

    .. note::

        This class is used to download files **only** from remote directory to the local one.

        It does NOT support direct file transfer between filesystems, like ``FTP -> SFTP``.
        You should use FileDownloader + :ref:`file-uploader` to implement ``FTP -> local dir -> SFTP``.

    .. versionadded:: 0.1.0

    .. versionchanged:: 0.8.0
        Moved ``onetl.core.FileDownloader`` → ``onetl.file.FileDownloader``

    Parameters
    ----------
    connection : :obj:`onetl.connection.FileConnection`
        Class which contains File system connection properties. See :ref:`file-connections` section.

    local_path : :obj:`os.PathLike` or :obj:`str`
        Local path where you download files

    source_path : :obj:`os.PathLike` or :obj:`str`, optional, default: ``None``
        Remote path to download files from.

        Could be ``None``, but only if you pass absolute file paths directly to
        :obj:`~run` method

    temp_path : :obj:`os.PathLike` or :obj:`str`, optional, default: ``None``
        If set, this path will be used for downloading a file, and then renaming it to the target file path.
        If ``None`` is passed, files are downloaded directly to ``target_path``.

        .. warning::

            In case of production ETL pipelines, please set a value for ``temp_path`` (NOT ``None``).
            This allows to properly handle download interruption,
            without creating half-downloaded files in the target,
            because unlike file download, ``rename`` call is atomic.

        .. warning::

            In case of connections like SFTP or FTP, which can have multiple underlying filesystems,
            please pass to ``temp_path`` path on the SAME filesystem as ``target_path``.
            Otherwise instead of ``rename``, remote OS will move file between filesystems,
            which is NOT atomic operation.

        .. versionadded:: 0.5.0

    filters : list of :obj:`BaseFileFilter <onetl.base.base_file_filter.BaseFileFilter>`
        Return only files/directories matching these filters. See :ref:`file-filters`

        .. versionchanged:: 0.3.0
            Replaces old ``source_path_pattern: str`` and ``exclude_dirs: str`` options.

        .. versionchanged:: 0.8.0
            Renamed ``filter`` → ``filters``

    limits : list of :obj:`BaseFileLimit <onetl.base.base_file_limit.BaseFileLimit>`
        Apply limits to the list of files/directories, and stop if one of the limits is reached.
        See :ref:`file-limits`

        .. versionadded:: 0.4.0

        .. versionchanged:: 0.8.0
            Renamed ``limit`` → ``limits``

    options : :obj:`~FileDownloader.Options`  | dict | None, default: ``None``
        File downloading options. See :obj:`FileDownloader.Options <onetl.file.file_downloader.options.FileDownloaderOptions>`

        .. versionadded:: 0.3.0

    hwm : type[HWM] | None, default: ``None``

        HWM class to detect changes in incremental run. See :etl-entities:`File HWM <hwm/file/index.html>`

        .. warning ::
            Used only in :obj:`IncrementalStrategy <onetl.strategy.incremental_strategy.IncrementalStrategy>`.

        .. versionadded:: 0.5.0

        .. versionchanged:: 0.10.0
            Replaces deprecated ``hwm_type`` attribute

    Examples
    --------

    .. tabs::

        .. code-tab:: py Minimal example

            from onetl.connection import SFTP
            from onetl.file import FileDownloader

            sftp = SFTP(...)

            # create downloader
            downloader = FileDownloader(
                connection=sftp,
                source_path="/path/to/remote/source",
                local_path="/path/to/local",
            )

            # download files to "/path/to/local"
            downloader.run()

        .. code-tab:: py Full example

            from onetl.connection import SFTP
            from onetl.file import FileDownloader
            from onetl.file.filter import Glob, ExcludeDir
            from onetl.file.limit import MaxFilesCount, TotalFileSize

            sftp = SFTP(...)

            # create downloader with a bunch of options
            downloader = FileDownloader(
                connection=sftp,
                source_path="/path/to/remote/source",
                local_path="/path/to/local",
                temp_path="/tmp",
                filters=[
                    Glob("*.txt"),
                    ExcludeDir("/path/to/remote/source/exclude_dir"),
                ],
                limits=[MaxFilesCount(100), TotalFileSize("10GiB")],
                options=FileDownloader.Options(delete_source=True, if_exists="replace_file"),
            )

            # download files to "/path/to/local",
            # but only *.txt,
            # excluding files from "/path/to/remote/source/exclude_dir" directory
            # and stop before downloading 101 file
            downloader.run()

        .. code-tab:: py Incremental download (by tracking list of file paths)

            from onetl.connection import SFTP
            from onetl.file import FileDownloader
            from onetl.strategy import IncrementalStrategy
            from etl_entities.hwm import FileListHWM

            sftp = SFTP(...)

            # create downloader
            downloader = FileDownloader(
                connection=sftp,
                source_path="/path/to/remote/source",
                local_path="/path/to/local",
                hwm=FileListHWM(  # mandatory for IncrementalStrategy
                    name="my_unique_hwm_name",
                ),
            )

            # download files to "/path/to/local", but only added since previous run
            with IncrementalStrategy():
                downloader.run()

        .. code-tab:: py Incremental download (by tracking file modification time)

            from onetl.connection import SFTP
            from onetl.file import FileDownloader
            from onetl.strategy import IncrementalStrategy
            from etl_entities.hwm import FileModifiedTimeHWM

            sftp = SFTP(...)

            # create downloader
            downloader = FileDownloader(
                connection=sftp,
                source_path="/path/to/remote/source",
                local_path="/path/to/local",
                hwm=FileModifiedTimeHWM(  # mandatory for IncrementalStrategy
                    name="my_unique_hwm_name",
                ),
            )

            # download files to "/path/to/local", but only modified/created since previous run
            with IncrementalStrategy():
                downloader.run()
    """

    Options = FileDownloaderOptions

    connection: BaseFileConnection

    local_path: LocalPath
    source_path: Optional[RemotePath] = None
    temp_path: Optional[LocalPath] = None

    filters: List[BaseFileFilter] = Field(default_factory=list, alias="filter")
    limits: List[BaseFileLimit] = Field(default_factory=list, alias="limit")

    hwm: Optional[FileHWM] = None
    hwm_type: Optional[Union[Type[OldFileListHWM], str]] = None

    options: FileDownloaderOptions = FileDownloaderOptions()

    _connection_checked: bool = PrivateAttr(default=False)

    @slot
    def run(self, files: Iterable[str | os.PathLike] | None = None) -> DownloadResult:  # noqa: WPS231
        """
        Method for downloading files from source to local directory. |support_hooks|

        .. note::

            This method can return different results depending on :ref:`strategy`

        .. versionadded:: 0.1.0

        Parameters
        ----------

        files : Iterable[str | os.PathLike] | None, default ``None``
            File list to download.

            If empty, download files from ``source_path`` to ``local_path``,
            applying ``filter``, ``limit`` and ``hwm`` to each one (if set).

            If not, download to ``local_path`` **all** input files, **ignoring**
            filters, limits and HWM.

            .. versionadded:: 0.3.0

        Returns
        -------
        :obj:`DownloadResult <onetl.file.file_downloader.download_result.DownloadResult>`

            Download result object

        Raises
        ------
        :obj:`onetl.exception.DirectoryNotFoundError`

            ``source_path`` does not found

        NotADirectoryError

            ``source_path`` or ``local_path`` is not a directory

        Examples
        --------

        Download files from ``source_path`` to ``local_path``:

        >>> from onetl.file import FileDownloader
        >>> downloader = FileDownloader(source_path="/remote", local_path="/local", ...)
        >>> download_result = downloader.run()
        >>> download_result
        DownloadResult(
            successful=FileSet([
                LocalPath("/local/file1.txt"),
                LocalPath("/local/file2.txt"),
                # directory structure is preserved
                LocalPath("/local/nested/path/file3.txt"),
            ]),
            failed=FileSet([
                FailedRemoteFile("/remote/failed.file"),
            ]),
            skipped=FileSet([
                RemoteFile("/remote/already.exists"),
            ]),
            missing=FileSet([
                RemotePath("/remote/missing.file"),
            ]),
        )

        Download only certain files from ``source_path``:

        >>> from onetl.file import FileDownloader
        >>> downloader = FileDownloader(source_path="/remote", local_path="/local", ...)
        >>> # paths could be relative or absolute, but all should be in "/remote"
        >>> download_result = downloader.run(
        ...     [
        ...         "/remote/file1.txt",
        ...         "/remote/nested/path/file3.txt",
        ...         # excluding "/remote/file2.txt"
        ...     ]
        ... )
        >>> download_result
        DownloadResult(
            successful=FileSet([
                LocalPath("/local/file1.txt"),
                # directory structure is preserved
                LocalPath("/local/nested/path/file3.txt"),
            ]),
            failed=FileSet([]),
            skipped=FileSet([]),
            missing=FileSet([]),
        )

        Download certain files from any folder:

        >>> from onetl.file import FileDownloader
        >>> downloader = FileDownloader(local_path="/local", ...)  # no source_path set
        >>> # only absolute paths
        >>> download_result = downloader.run(
        ...     [
        ...         "/remote/file1.txt",
        ...         "/any/nested/path/file2.txt",
        ...     ]
        ... )
        >>> download_result
        DownloadResult(
            successful=FileSet([
                LocalPath("/local/file1.txt"),
                # directory structure is NOT preserved without source_path
                LocalPath("/local/file2.txt"),
            ]),
            failed=FileSet([]),
            skipped=FileSet([]),
            missing=FileSet([]),
        )
        """

        entity_boundary_log(log, f"{self.__class__.__name__}.run() starts")

        if not self._connection_checked:
            self._log_parameters(files)

        self._check_strategy()

        if files is None and not self.source_path:
            raise ValueError("Neither file list nor `source_path` are passed")

        # Check everything
        if not self._connection_checked:
            self._check_local_path()
            self.connection.check()

            if self.source_path:
                self._check_source_path()

            self._connection_checked = True

        if files is None:
            log.debug("|%s| File list is not passed to `run` method", self.__class__.__name__)
            files = self.view_files()

        if not files:
            log.info("|%s| No files to download!", self.__class__.__name__)
            return DownloadResult()

        current_temp_dir: LocalPath | None = None
        if self.temp_path:
            current_temp_dir = generate_temp_path(self.temp_path)

        to_download = self._validate_files(files, current_temp_dir=current_temp_dir)

        # remove folder only after everything is checked
        if self.options.if_exists == FileExistBehavior.REPLACE_ENTIRE_DIRECTORY:
            if self.local_path.exists():
                shutil.rmtree(self.local_path)
            self.local_path.mkdir()

        if self.hwm:
            self._init_hwm(self.hwm)

        result = self._download_files(to_download)
        if current_temp_dir:
            self._remove_temp_dir(current_temp_dir)

        self._log_result(result)
        entity_boundary_log(log, f"{self.__class__.__name__}.run() ends", char="-")
        return result

    @slot
    def view_files(self) -> FileSet[RemoteFile]:
        """
        Get file list in the ``source_path``,
        after ``filter``, ``limit`` and ``hwm`` applied (if any). |support_hooks|

        .. note::

            This method can return different results depending on :ref:`strategy`

        .. versionadded:: 0.3.0

        Raises
        ------
        :obj:`onetl.exception.DirectoryNotFoundError`

            ``source_path`` does not found

        NotADirectoryError

            ``source_path`` is not a directory

        Returns
        -------
        FileSet[RemoteFile]
            Set of files in ``source_path``, which will be downloaded by :obj:`~run` method

        Examples
        --------

        View files:

        >>> from onetl.file import FileDownloader
        >>> downloader = FileDownloader(source_path="/remote", ...)
        >>> downloader.view_files()
        FileSet([
            RemoteFile("/remote/file1.txt"),
            RemoteFile("/remote/file3.txt"),
            RemoteFile("/remote/nested/file3.txt"),
        ])
        """

        if not self.source_path:
            raise ValueError("Cannot call `.view_files()` without `source_path`")

        log.debug("|%s| Getting files list from path '%s'", self.connection.__class__.__name__, self.source_path)

        if not self._connection_checked:
            self._check_source_path()

        filters = self.filters.copy()
        if self.hwm:
            filters.append(FileHWMFilter(hwm=self._init_hwm(self.hwm)))

        result = FileSet()
        try:
            for root, _dirs, files in self.connection.walk(self.source_path, filters=filters, limits=self.limits):
                for file in files:
                    result.append(RemoteFile(path=root / file, stats=file.stat()))

        except Exception as e:
            raise RuntimeError(
                f"Couldn't read directory tree from remote dir '{self.source_path}'",
            ) from e

        return result

    @validator("local_path", pre=True, always=True)
    def _resolve_local_path(cls, local_path):
        return LocalPath(local_path).resolve()

    @validator("source_path", pre=True, always=True)
    def _validate_source_path(cls, source_path):
        return RemotePath(source_path) if source_path else None

    @validator("temp_path", pre=True, always=True)
    def _validate_temp_path(cls, temp_path):
        return LocalPath(temp_path).resolve() if temp_path else None

    @root_validator(skip_on_failure=True)
    def _validate_hwm(cls, values):
        connection = values["connection"]
        source_path = values.get("source_path")
        hwm_type = values.get("hwm_type")
        hwm = values.get("hwm")

        if (hwm or hwm_type) and not source_path:
            raise ValueError("If `hwm` is passed, `source_path` must be specified")

        if hwm_type and (hwm_type == "file_list" or issubclass(hwm_type, OldFileListHWM)):
            remote_file_folder = RemoteFolder(name=source_path, instance=connection.instance_url)
            old_hwm = OldFileListHWM(source=remote_file_folder)
            warnings.warn(
                textwrap.dedent(
                    f"""
                    Passing "hwm_type" to FileDownloader class is deprecated since version 0.10.0,
                    and will be removed in v1.0.0.

                    Instead use:
                        hwm=FileListHWM(name={old_hwm.qualified_name!r})
                    """,
                ),
                UserWarning,
                stacklevel=2,
            )
            hwm = FileListHWM(
                name=old_hwm.qualified_name,
                directory=source_path,
            )

        if hwm and not hwm.entity:
            hwm = hwm.copy(update={"entity": AbsolutePath(source_path)})

        if hwm and hwm.entity != source_path:
            error_message = textwrap.dedent(
                f"""
                Passed `hwm.directory` is different from `source_path`.

                `hwm`:
                    {hwm!r}

                `source_path`:
                    {source_path!r}

                This is not allowed.
                """,
            )
            raise ValueError(error_message)

        values["hwm"] = hwm
        values["hwm_type"] = None
        return values

    @validator("filters", pre=True)
    def _validate_filters(cls, filters):
        if filters is None:
            warnings.warn(
                "filter=None is deprecated in v0.8.0, use filters=[] instead",
                UserWarning,
                stacklevel=3,
            )
            return []

        if not isinstance(filters, list):
            warnings.warn(
                "filter=... is deprecated in v0.8.0, use filters=[...] instead",
                UserWarning,
                stacklevel=3,
            )
            filters = [filters]

        return filters

    @validator("limits", pre=True)
    def _validate_limits(cls, limits):
        if limits is None:
            warnings.warn(
                "limit=None is deprecated in v0.8.0, use limits=[] instead",
                UserWarning,
                stacklevel=3,
            )
            return []

        if not isinstance(limits, list):
            warnings.warn(
                "limit=... is deprecated in v0.8.0, use limits=[...] instead",
                UserWarning,
                stacklevel=3,
            )
            limits = [limits]

        return limits

    def _check_strategy(self):
        strategy = StrategyManager.get_current()
        class_name = self.__class__.__name__
        strategy_name = strategy.__class__.__name__

        if self.hwm:
            if not isinstance(strategy, HWMStrategy):
                raise ValueError(f"{class_name}(hwm=...) cannot be used with {strategy_name}")

            offset = getattr(strategy, "offset", None)
            if offset is not None:
                raise ValueError(f"{class_name}(hwm=...) cannot be used with {strategy_name}(offset={offset}, ...)")

            if isinstance(strategy, BatchHWMStrategy):
                raise ValueError(f"{class_name}(hwm=...) cannot be used with {strategy_name}")

    def _init_hwm(self, hwm: FileHWM) -> FileHWM:
        strategy: HWMStrategy = StrategyManager.get_current()

        if not strategy.hwm:
            strategy.hwm = self.hwm
            strategy.fetch_hwm()
            return strategy.hwm

        if not isinstance(strategy.hwm, FileHWM) or strategy.hwm.name != hwm.name:
            # exception raised when inside one strategy >1 processes on the same table but with different hwm columns
            # are executed, example: test_postgres_strategy_incremental_hwm_set_twice
            error_message = textwrap.dedent(
                f"""
                Detected wrong {type(strategy).__name__} usage.

                Previous run:
                    {strategy.hwm!r}
                Current run:
                    {self.hwm!r}

                Probably you've executed code which looks like this:
                    with {strategy.__class__.__name__}(...):
                        FileDownloader(hwm=one_hwm, ...).run()
                        FileDownloader(hwm=another_hwm, ...).run()

                Please change it to:
                    with {strategy.__class__.__name__}(...):
                        FileDownloader(hwm=one_hwm, ...).run()

                    with {strategy.__class__.__name__}(...):
                        FileDownloader(hwm=another_hwm, ...).run()
                """,
            )
            raise ValueError(error_message)

        strategy.validate_hwm_attributes(hwm, strategy.hwm, origin=self.__class__.__name__)
        return strategy.hwm

    def _log_parameters(self, files: Iterable[str | os.PathLike] | None = None) -> None:
        log.info("|%s| -> |Local FS| Downloading files using parameters:", self.connection.__class__.__name__)
        log_with_indent(log, "source_path = %s", f"'{self.source_path}'" if self.source_path else "None")
        log_with_indent(log, "local_path = '%s'", self.local_path)
        log_with_indent(log, "temp_path = %s", f"'{self.temp_path}'" if self.temp_path else "None")
        log_collection(log, "filters", self.filters)
        log_collection(log, "limits", self.limits)
        if self.hwm:
            log_hwm(log, self.hwm)
        log_options(log, self.options.dict(by_alias=True))

        if self.options.delete_source:
            log.warning("|%s| SOURCE FILES WILL BE PERMANENTLY DELETED AFTER DOWNLOADING !!!", self.__class__.__name__)

        if self.options.if_exists == FileExistBehavior.REPLACE_ENTIRE_DIRECTORY:
            log.warning("|%s| LOCAL DIRECTORY WILL BE CLEANED UP BEFORE DOWNLOADING FILES !!!", self.__class__.__name__)

        if files and self.source_path:
            log.warning(
                "|%s| Passed both `source_path` and files list at the same time. Using explicit files list",
                self.__class__.__name__,
            )

    def _validate_files(  # noqa: WPS231
        self,
        remote_files: Iterable[os.PathLike | str],
        current_temp_dir: LocalPath | None,
    ) -> DOWNLOAD_ITEMS_TYPE:
        result: DOWNLOAD_ITEMS_TYPE = OrderedSet()

        for file in remote_files:
            remote_file_path = file if isinstance(file, PathProtocol) else RemotePath(file)
            remote_file = remote_file_path
            tmp_file: LocalPath | None = None

            if not self.source_path:
                # Download into a flat structure
                if not remote_file_path.is_absolute():
                    raise ValueError("Cannot pass relative file path with empty `source_path`")

                filename = remote_file_path.name
                local_file = self.local_path / filename
                if current_temp_dir:
                    tmp_file = current_temp_dir / filename  # noqa: WPS220
            else:
                # Download according to source folder structure
                if self.source_path in remote_file_path.parents:
                    # Make relative local path
                    local_file = self.local_path / remote_file_path.relative_to(self.source_path)
                    if current_temp_dir:
                        tmp_file = current_temp_dir / remote_file_path.relative_to(self.source_path)  # noqa: WPS220

                elif not remote_file_path.is_absolute():
                    # Passed path is already relative
                    local_file = self.local_path / remote_file_path
                    remote_file = self.source_path / remote_file_path
                    if current_temp_dir:
                        tmp_file = current_temp_dir / remote_file_path  # noqa: WPS220
                else:
                    # Wrong path (not relative path and source path not in the path to the file)
                    raise ValueError(f"File path '{remote_file}' does not match source_path '{self.source_path}'")

            if not isinstance(remote_file, PathProtocol) and self.connection.path_exists(remote_file):
                remote_file = self.connection.resolve_file(remote_file)

            result.add((remote_file, local_file, tmp_file))

        return result

    def _check_source_path(self):
        self.connection.resolve_dir(self.source_path)

    def _check_local_path(self):
        if self.local_path.exists() and not self.local_path.is_dir():
            raise NotADirectoryError(f"{path_repr(self.local_path)} is not a directory")

        self.local_path.mkdir(exist_ok=True, parents=True)

    def _download_files(  # noqa: WPS231
        self,
        to_download: DOWNLOAD_ITEMS_TYPE,
    ) -> DownloadResult:
        files = FileSet(item[0] for item in to_download)
        log.info("|%s| Files to be downloaded:", self.__class__.__name__)
        log_lines(log, str(files))
        log_with_indent(log, "")
        log.info("|%s| Starting the download process...", self.__class__.__name__)

        self._create_dirs(to_download)

        strategy = StrategyManager.get_current()
        result = DownloadResult()
        source_files: list[RemotePath] = []
        try:  # noqa: WPS501
            for status, source_file, target_file in self._bulk_download(to_download):
                if status == FileDownloadStatus.SUCCESSFUL:
                    result.successful.add(target_file)
                    source_files.append(source_file)
                elif status == FileDownloadStatus.FAILED:
                    result.failed.add(source_file)
                elif status == FileDownloadStatus.SKIPPED:
                    result.skipped.add(source_file)
                elif status == FileDownloadStatus.MISSING:
                    result.missing.add(source_file)
        finally:
            if self.hwm:
                # always update HWM in HWM store, even if downloader is interrupted
                strategy.update_hwm(source_files)
                strategy.save_hwm()
        return result

    def _create_dirs(
        self,
        to_download: DOWNLOAD_ITEMS_TYPE,
    ) -> None:
        """
        Create all parent paths before downloading files
        This is required to avoid errors then multiple threads create the same dir
        """
        parent_paths = OrderedSet()
        for _, target_file, tmp_file in to_download:
            parent_paths.add(target_file.parent)
            if tmp_file:
                parent_paths.add(tmp_file.parent)

        for parent_path in parent_paths:
            parent_path.mkdir(parents=True, exist_ok=True)

    def _bulk_download(
        self,
        to_download: DOWNLOAD_ITEMS_TYPE,
    ) -> Generator[tuple[FileDownloadStatus, RemotePath, LocalPath | None], None, None]:
        workers = self.options.workers
        files_count = len(to_download)

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
                max_workers=real_workers,
                thread_name_prefix=self.__class__.__name__,
            ) as executor:
                futures = [
                    executor.submit(self._download_file, source_file, target_file, tmp_file)
                    for source_file, target_file, tmp_file in to_download
                ]
                yield from (future.result() for future in as_completed(futures))
        else:
            log.debug("|%s| Using plain old for-loop", self.__class__.__name__)
            yield from (
                self._download_file(source_file, target_file, tmp_file)
                for source_file, target_file, tmp_file in to_download
            )

    def _download_file(  # noqa: WPS231, WPS213
        self,
        source_file: RemotePath,
        local_file: LocalPath,
        tmp_file: LocalPath | None,
    ) -> tuple[FileDownloadStatus, RemotePath, LocalPath | None]:
        if tmp_file:
            log.info(
                "|%s| Downloading file '%s' to '%s' (via tmp '%s')",
                self.__class__.__name__,
                source_file,
                local_file,
                tmp_file,
            )
        else:
            log.info("|%s| Downloading file '%s' to '%s'", self.__class__.__name__, source_file, local_file)

        if not self.connection.path_exists(source_file):
            log.warning("|%s| Missing file '%s', skipping", self.__class__.__name__, source_file)
            return FileDownloadStatus.MISSING, source_file, None

        try:
            remote_file = self.connection.resolve_file(source_file)

            replace = False
            if local_file.exists():
                if self.options.if_exists == FileExistBehavior.ERROR:
                    raise FileExistsError(f"File {path_repr(local_file)} already exists")

                if self.options.if_exists == FileExistBehavior.IGNORE:
                    log.warning("|Local FS| File %s already exists, skipping", path_repr(local_file))
                    return FileDownloadStatus.SKIPPED, remote_file, None

                replace = True

            if tmp_file:
                # Files are loaded to temporary directory before moving them to target dir.
                # This prevents operations with partly downloaded files

                self.connection.download_file(remote_file, tmp_file, replace=replace)

                # remove existing file only after new file is downloaded
                # to avoid issues then there is no free space to download new file, but existing one is already gone
                if replace and local_file.exists():
                    log.warning("|Local FS| File %s already exists, overwriting", path_repr(local_file))
                    local_file.unlink()

                local_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(tmp_file, local_file)
            else:
                # Direct download
                self.connection.download_file(remote_file, local_file, replace=replace)

            # Delete Remote
            if self.options.delete_source:
                self.connection.remove_file(remote_file)

            return FileDownloadStatus.SUCCESSFUL, remote_file, local_file

        except Exception as e:
            if log.isEnabledFor(logging.DEBUG):
                log.exception(
                    "|%s| Couldn't download file from source dir",
                    self.__class__.__name__,
                    exc_info=e,
                )
            else:
                log.exception(
                    "|%s| Couldn't download file from source dir: %s",
                    self.__class__.__name__,
                    e,
                    exc_info=False,
                )
            failed_file = FailedRemoteFile(
                path=remote_file.path,
                stats=remote_file.stats,
                exception=e,
            )
            return FileDownloadStatus.FAILED, failed_file, None

    def _remove_temp_dir(self, temp_dir: LocalPath) -> None:
        log.info("|Local FS| Removing temp directory '%s'", temp_dir)

        try:
            shutil.rmtree(temp_dir)
        except Exception:
            log.exception("|Local FS| Error while removing temp directory")

    def _log_result(self, result: DownloadResult) -> None:
        log_with_indent(log, "")
        log.info("|%s| Download result:", self.__class__.__name__)
        log_lines(log, str(result))
