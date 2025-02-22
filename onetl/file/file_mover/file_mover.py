# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from typing import Iterable, List, Optional, Tuple

from ordered_set import OrderedSet

try:
    from pydantic.v1 import Field, PrivateAttr, validator
except (ImportError, AttributeError):
    from pydantic import Field, PrivateAttr, validator  # type: ignore[no-redef, assignment]

from onetl.base import BaseFileConnection, BaseFileFilter, BaseFileLimit
from onetl.base.path_protocol import PathProtocol, PathWithStatsProtocol
from onetl.base.pure_path_protocol import PurePathProtocol
from onetl.file.file_mover.options import FileMoverOptions
from onetl.file.file_mover.result import MoveResult
from onetl.file.file_set import FileSet
from onetl.hooks import slot, support_hooks
from onetl.impl import (
    FailedRemoteFile,
    FileExistBehavior,
    FrozenModel,
    RemoteFile,
    RemotePath,
    path_repr,
)
from onetl.log import (
    entity_boundary_log,
    log_collection,
    log_lines,
    log_options,
    log_with_indent,
)

log = logging.getLogger(__name__)

# source, target
MOVE_ITEMS_TYPE = OrderedSet[Tuple[RemotePath, RemotePath]]


class FileMoveStatus(Enum):
    SUCCESSFUL = 0
    FAILED = 1
    SKIPPED = 2
    MISSING = -1


@support_hooks
class FileMover(FrozenModel):
    """Allows you to move files between different directories in a filesystem,
    and return an object with move result summary. |support_hooks|

    .. note::

        This class is used to move files **only** within the same connection,

        It does NOT support direct file transfer between filesystems, like ``FTP -> SFTP``.
        You should use :ref:`file-downloader` + :ref:`file-uploader` to implement ``FTP -> local dir -> SFTP``.

    .. warning::

        This class does **not** support read strategies.

    .. versionadded:: 0.8.0

    Parameters
    ----------
    connection : :obj:`onetl.connection.FileConnection`
        Class which contains File system connection properties. See :ref:`file-connections` section.

    target_path : :obj:`os.PathLike` or :obj:`str`
        Remote path to move files to

    source_path : :obj:`os.PathLike` or :obj:`str`, optional, default: ``None``
        Remote path to move files from.

        Could be ``None``, but only if you pass absolute file paths directly to
        :obj:`~run` method

    filters : list of :obj:`BaseFileFilter <onetl.base.base_file_filter.BaseFileFilter>`
        Return only files/directories matching these filters. See :ref:`file-filters`

    limits : list of :obj:`BaseFileLimit <onetl.base.base_file_limit.BaseFileLimit>`
        Apply limits to the list of files/directories, and stop if one of the limits is reached.
        See :ref:`file-limits`

    options : :obj:`~FileMover.Options`  | dict | None, default: ``None``
        File moving options. See :obj:`FileMover.Options <onetl.file.file_mover.options.FileMoverOptions>`

    Examples
    --------

    .. tabs::

        .. code-tab:: py Minimal example

            from onetl.connection import SFTP
            from onetl.file import FileMover

            sftp = SFTP(...)

            # create mover
            mover = FileMover(
                connection=sftp,
                source_path="/path/to/source/dir",
                target_path="/path/to/target/dir",
            )

            # move files from "/path/to/source/dir" to "/path/to/target/dir"
            mover.run()

        .. code-tab:: py Full example

            from onetl.connection import SFTP
            from onetl.file import FileMover
            from onetl.file.filter import Glob, ExcludeDir
            from onetl.file.limit import MaxFilesCount, TotalFilesSize

            sftp = SFTP(...)

            # create mover with a bunch of options
            mover = FileMover(
                connection=sftp,
                source_path="/path/to/source/dir",
                target_path="/path/to/target/dir",
                filters=[
                    Glob("*.txt"),
                    ExcludeDir("/path/to/source/dir/exclude"),
                ],
                limits=[MaxFilesCount(100), TotalFileSize("10GiB")],
                options=FileMover.Options(if_exists="replace_file"),
            )

            # move files from "/path/to/source/dir" to "/path/to/target/dir",
            # but only *.txt files
            # excluding files from "/path/to/source/dir/exclude" directory
            # and stop before downloading 101 file
            mover.run()

    """

    Options = FileMoverOptions

    connection: BaseFileConnection

    target_path: RemotePath
    source_path: Optional[RemotePath] = None

    filters: List[BaseFileFilter] = Field(default_factory=list)
    limits: List[BaseFileLimit] = Field(default_factory=list)

    options: FileMoverOptions = FileMoverOptions()

    _connection_checked: bool = PrivateAttr(default=False)

    @slot
    def run(self, files: Iterable[str | os.PathLike] | None = None) -> MoveResult:  # noqa: WPS231
        """
        Method for moving files from source to target directory. |support_hooks|

        .. versionadded:: 0.8.0

        Parameters
        ----------

        files : Iterable[str | os.PathLike] | None, default ``None``
            File list to move.

            If empty, move files from ``source_path`` to ``target_path``,
            applying ``filter`` and ``limit`` to each one (if set).

            If not, move to ``target_path`` **all** input files, **without**
            any filtering and limiting.

        Returns
        -------
        :obj:`MoveResult <onetl.file.file_mover.move_result.MoveResult>`

            Move result object

        Raises
        ------
        :obj:`onetl.exception.DirectoryNotFoundError`

            ``source_path`` does not found

        NotADirectoryError

            ``source_path`` or ``target_path`` is not a directory

        Examples
        --------

        Move files from ``source_path``:

        >>> from onetl.file import FileMover
        >>> mover = FileMover(source_path="/source", target_path="/target", ...)
        >>> move_result = mover.run()
        >>> move_result
        MoveResult(
            successful=FileSet([
                RemoteFile("/target/file1.txt"),
                RemoteFile("/target/file2.txt"),
                # directory structure is preserved
                RemoteFile("/target/nested/path/file3.txt"),
            ]),
            failed=FileSet([
                FailedRemoteFile("/source/failed.file"),
            ]),
            skipped=FileSet([
                RemoteFile("/source/already.exists"),
            ]),
            missing=FileSet([
                RemotePath("/source/missing.file"),
            ]),
        )

        Move only certain files from ``source_path``:

        >>> from onetl.file import FileMover
        >>> mover = FileMover(source_path="/source", target_path="/target", ...)
        >>> # paths could be relative or absolute, but all should be in "/source"
        >>> move_result = mover.run(
        ...     [
        ...         "/source/file1.txt",
        ...         "/source/nested/path/file3.txt",
        ...         # excluding "/source/file2.txt"
        ...     ]
        ... )
        >>> move_result
        MoveResult(
            successful=FileSet([
                RemoteFile("/target/file1.txt"),
                # directory structure is preserved
                RemoteFile("/target/nested/path/file3.txt"),
            ]),
            failed=FileSet([]),
            skipped=FileSet([]),
            missing=FileSet([]),
        )

        Move certain files from any folder:

        >>> from onetl.file import FileMover
        >>> mover = FileMover(target_path="/target", ...)  # no source_path set
        >>> # only absolute paths
        >>> move_result = mover.run(
        ...     [
        ...         "/remote/file1.txt",
        ...         "/any/nested/path/file3.txt",
        ...     ]
        ... )
        >>> move_result
        MoveResult(
            successful=FileSet([
                RemoteFile("/target/file1.txt"),
                # directory structure is NOT preserved without source_path
                RemoteFile("/target/file3.txt"),
            ]),
            failed=FileSet([]),
            skipped=FileSet([]),
            missing=FileSet([]),
        )
        """

        entity_boundary_log(log, f"{self.__class__.__name__}.run() starts")

        if files is None and not self.source_path:
            raise ValueError("Neither file list nor `source_path` are passed")

        if not self._connection_checked:
            self._log_parameters(files)

            self.connection.check()
            self._check_target_path()
            log_with_indent(log, "")

            if self.source_path:
                self._check_source_path()

            self._connection_checked = True

        if files is None:
            log.debug("|%s| File list is not passed to `run` method", self.__class__.__name__)
            files = self.view_files()

        if not files:
            log.info("|%s| No files to move!", self.__class__.__name__)
            return MoveResult()

        to_move = self._validate_files(files)

        # remove folder only after everything is checked
        if self.options.if_exists == FileExistBehavior.REPLACE_ENTIRE_DIRECTORY:
            self.connection.remove_dir(self.target_path, recursive=True)
            self.connection.create_dir(self.target_path)

        result = self._move_files(to_move)
        self._log_result(result)
        entity_boundary_log(log, f"{self.__class__.__name__}.run() ends", char="-")
        return result

    @slot
    def view_files(self) -> FileSet[RemoteFile]:
        """
        Get file list in the ``source_path``,
        after ``filter`` and ``limit`` applied (if any). |support_hooks|

        .. versionadded:: 0.8.0

        Raises
        ------
        :obj:`onetl.exception.DirectoryNotFoundError`

            ``source_path`` does not found

        NotADirectoryError

            ``source_path`` is not a directory

        Returns
        -------
        FileSet[RemoteFile]
            Set of files in ``source_path``, which will be moved by :obj:`~run` method

        Examples
        --------

        View files:

        >>> from onetl.file import FileMover
        >>> mover = FileMover(source_path="/remote", ...)
        >>> mover.view_files()
        FileSet([
            RemoteFile("/remote/file1.txt"),
            RemoteFile("/remote/file2.txt"),
            RemoteFile("/remote/nested/path/file3.txt"),
        ])
        """

        if not self.source_path:
            raise ValueError("Cannot call `.view_files()` without `source_path`")

        log.debug("|%s| Getting files list from path '%s'", self.connection.__class__.__name__, self.source_path)

        if not self._connection_checked:
            self._check_source_path()

        result = FileSet()

        try:
            for root, _dirs, files in self.connection.walk(self.source_path, filters=self.filters, limits=self.limits):
                for file in files:
                    result.append(RemoteFile(path=root / file, stats=file.stat()))

        except Exception as e:
            raise RuntimeError(
                f"Couldn't read directory tree from remote dir '{self.source_path}'",
            ) from e

        return result

    def _log_parameters(self, files: Iterable[str | os.PathLike] | None = None) -> None:
        connection_class = self.connection.__class__.__name__
        log.info("|%s| -> |%s| Moving files using parameters:", connection_class, connection_class)
        log_with_indent(log, "source_path = %s", f"'{self.source_path}'" if self.source_path else "None")
        log_with_indent(log, "target_path = '%s'", self.target_path)
        log_collection(log, "filters", self.filters)
        log_collection(log, "limits", self.limits)
        log_options(log, self.options.dict(by_alias=True))

        if self.options.if_exists == FileExistBehavior.REPLACE_ENTIRE_DIRECTORY:
            log.warning("|%s| TARGET DIRECTORY WILL BE CLEANED UP BEFORE MOVING FILES !!!", self.__class__.__name__)

        if files and self.source_path:
            log.warning(
                "|%s| Passed both `source_path` and files list at the same time. Using explicit files list",
                self.__class__.__name__,
            )

    def _validate_files(  # noqa: WPS231
        self,
        remote_files: Iterable[os.PathLike | str],
    ) -> MOVE_ITEMS_TYPE:
        result = OrderedSet()

        for file in remote_files:
            remote_file_path = file if isinstance(file, PathProtocol) else RemotePath(file)
            old_file = remote_file_path

            if not self.source_path:
                # Move into a flat structure
                if not remote_file_path.is_absolute():
                    raise ValueError("Cannot pass relative file path with empty `source_path`")

                filename = remote_file_path.name
                new_file = self.target_path / filename
            else:
                # Move according to source folder structure
                if self.source_path in remote_file_path.parents:
                    # Make relative local path
                    new_file = self.target_path / remote_file_path.relative_to(self.source_path)

                elif not remote_file_path.is_absolute():
                    # Passed path is already relative
                    new_file = self.target_path / remote_file_path
                    old_file = self.source_path / remote_file_path
                else:
                    # Wrong path (not relative path and source path not in the path to the file)
                    raise ValueError(f"File path '{old_file}' does not match source_path '{self.source_path}'")

            if not isinstance(old_file, PathProtocol) and self.connection.path_exists(old_file):
                old_file = self.connection.resolve_file(old_file)

            result.add((old_file, new_file))

        return result

    @validator("target_path", pre=True, always=True)
    def _resolve_target_path(cls, target_path):
        return RemotePath(target_path)

    @validator("source_path", pre=True, always=True)
    def _validate_source_path(cls, source_path):
        return RemotePath(source_path) if source_path else None

    def _check_source_path(self):
        self.connection.resolve_dir(self.source_path)

    def _check_target_path(self):
        self.connection.create_dir(self.target_path)

    def _move_files(
        self,
        to_move: MOVE_ITEMS_TYPE,
    ) -> MoveResult:
        files = FileSet(item[0] for item in to_move)
        log.info("|%s| Files to be moved:", self.__class__.__name__)
        log_lines(log, str(files))
        log_with_indent(log, "")
        log.info("|%s| Starting the move process...", self.__class__.__name__)

        self._create_dirs(to_move)

        result = MoveResult()
        for status, file in self._bulk_move(to_move):
            if status == FileMoveStatus.SUCCESSFUL:
                result.successful.add(file)
            elif status == FileMoveStatus.FAILED:
                result.failed.add(file)
            elif status == FileMoveStatus.SKIPPED:
                result.skipped.add(file)
            elif status == FileMoveStatus.MISSING:
                result.missing.add(file)

        return result

    def _create_dirs(
        self,
        to_move: MOVE_ITEMS_TYPE,
    ) -> None:
        """
        Create all parent paths before moving files
        This is required to avoid errors then multiple threads create the same dir
        """
        parent_paths = OrderedSet(target_file.parent for _, target_file in to_move)
        for parent_path in parent_paths:
            self.connection.create_dir(parent_path)

    def _bulk_move(
        self,
        to_move: MOVE_ITEMS_TYPE,
    ) -> list[tuple[FileMoveStatus, PurePathProtocol | PathWithStatsProtocol]]:
        workers = self.options.workers
        files_count = len(to_move)
        result = []

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
                max_workers=workers,
                thread_name_prefix=self.__class__.__name__,
            ) as executor:
                futures = [
                    executor.submit(self._move_file, source_file, target_file) for source_file, target_file in to_move
                ]
                for future in as_completed(futures):
                    result.append(future.result())
        else:
            log.debug("|%s| Using plain old for-loop", self.__class__.__name__)
            for source_file, target_file in to_move:
                result.append(
                    self._move_file(
                        source_file,
                        target_file,
                    ),
                )

        return result

    def _move_file(  # noqa: WPS231, WPS213
        self,
        source_file: RemotePath,
        target_file: RemotePath,
    ) -> tuple[FileMoveStatus, PurePathProtocol | PathWithStatsProtocol]:
        log.info("|%s| Moving file '%s' to '%s'", self.__class__.__name__, source_file, target_file)

        if not self.connection.path_exists(source_file):
            log.warning("|%s| Missing file '%s', skipping", self.__class__.__name__, source_file)
            return FileMoveStatus.MISSING, source_file

        try:
            replace = False
            if self.connection.path_exists(target_file):
                new_file = self.connection.resolve_file(target_file)

                if self.options.if_exists == FileExistBehavior.ERROR:
                    raise FileExistsError(f"File {path_repr(new_file)} already exists")

                if self.options.if_exists == FileExistBehavior.IGNORE:
                    log.warning(
                        "|%s| File %s already exists, skipping",
                        self.connection.__class__.__name__,
                        path_repr(new_file),
                    )
                    return FileMoveStatus.SKIPPED, source_file

                replace = True

            new_file = self.connection.rename_file(source_file, target_file, replace=replace)
            return FileMoveStatus.SUCCESSFUL, new_file

        except Exception as e:
            if log.isEnabledFor(logging.DEBUG):
                log.exception(
                    "|%s| Couldn't move file to target dir",
                    self.__class__.__name__,
                    exc_info=e,
                )
            else:
                log.exception(
                    "|%s| Couldn't move file to target dir: %s",
                    self.__class__.__name__,
                    e,
                    exc_info=False,
                )
            return FileMoveStatus.FAILED, FailedRemoteFile(path=source_file.path, stats=source_file.stats, exception=e)

    def _log_result(self, result: MoveResult) -> None:
        log_with_indent(log, "")
        log.info("|%s| Move result:", self.__class__.__name__)
        log_lines(log, str(result))
