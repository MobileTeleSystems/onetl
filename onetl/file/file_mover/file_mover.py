#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import logging
import os
from typing import Iterable, List, Optional, Tuple

from ordered_set import OrderedSet
from pydantic import Field, validator

from onetl.base import BaseFileConnection, BaseFileFilter, BaseFileLimit
from onetl.base.path_protocol import PathProtocol
from onetl.file.file_mover.move_result import MoveResult
from onetl.file.file_set import FileSet
from onetl.impl import (
    FailedRemoteFile,
    FileWriteMode,
    FrozenModel,
    GenericOptions,
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


class FileMover(FrozenModel):
    """Allows you to move files between different directories in a filesystem,
    and return an object with move result summary.

    .. note::

        This class is used to move files **only** within the same connection,

        It does NOT support direct file transfer between filesystems, like ``FTP -> SFTP``.
        You should use :ref:`file-downloader` + :ref:`file-uploader` to implement ``FTP -> local dir -> SFTP``.

    .. warning::

        This class does **not** support read strategies.

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
        File moving options. See :obj:`~FileMover.Options`

    Examples
    --------
    Simple Mover creation

    .. code:: python

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

    Mover with all parameters

    .. code:: python

        from onetl.connection import SFTP
        from onetl.file import FileMover
        from onetl.file.filter import Glob, ExcludeDir
        from onetl.file.limit import MaxFilesCount

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
            limits=[MaxFilesCount(100)],
            options=FileMover.Options(mode="overwrite"),
        )

        # move files from "/path/to/source/dir" to "/path/to/target/dir",
        # but only *.txt files
        # excluding files from "/path/to/source/dir/exclude" directory
        # and stop before downloading 101 file
        mover.run()

    """

    class Options(GenericOptions):
        """File moving options"""

        mode: FileWriteMode = FileWriteMode.ERROR
        """
        How to handle existing files in the local directory.

        Possible values:
            * ``error`` (default) - do nothing, mark file as failed
            * ``ignore`` - do nothing, mark file as ignored
            * ``overwrite`` - replace existing file with a new one
            * ``delete_all`` - delete directory content before moving files
        """

    connection: BaseFileConnection

    target_path: RemotePath
    source_path: Optional[RemotePath] = None

    filters: List[BaseFileFilter] = Field(default_factory=list)
    limits: List[BaseFileLimit] = Field(default_factory=list)

    options: Options = Options()

    def run(self, files: Iterable[str | os.PathLike] | None = None) -> MoveResult:  # noqa: WPS231
        """
        Method for moving files from source to target directory.

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
        moved_files : :obj:`MoveResult <onetl.file.file_mover.move_result.MoveResult>`

            Move result object

        Raises
        -------
        :obj:`onetl.exception.DirectoryNotFoundError`

            ``source_path`` does not found

        NotADirectoryError

            ``source_path`` or ``target_path`` is not a directory

        Examples
        --------

        Move files from ``source_path``

        .. code:: python

            from onetl.impl import RemoteFile, RemotePath
            from onetl.file import FileMover

            mover = FileMover(source_path="/source", target_path="/target", ...)
            moved_files = mover.run()

            assert moved_files.successful == {
                RemoteFile("/target/file1.txt"),
                RemoteFile("/target/file2.txt"),
                RemoteFile("/target/nested/path/file3.txt"),  # directory structure is preserved
            }
            assert moved_files.failed == {FailedRemoteFile("/source/failed.file")}
            assert moved_files.skipped == {RemoteFile("/source/already.exists")}
            assert moved_files.missing == {RemotePath("/source/missing.file")}

        Move only certain files from ``source_path``

        .. code:: python

            from onetl.impl import RemoteFile
            from onetl.file import FileMover

            mover = FileMover(source_path="/source", target_path="/target", ...)

            # paths could be relative or absolute, but all should be in "/source"
            moved_files = mover.run(
                [
                    "/source/file1.txt",
                    "/source/nested/path/file3.txt",
                    # excluding "/source/file2.txt"
                ]
            )

            assert moved_files.successful == {
                RemoteFile("/target/file1.txt"),
                RemoteFile("/target/nested/path/file3.txt"),  # directory structure is preserved
            }
            assert not moved_files.failed
            assert not moved_files.skipped
            assert not moved_files.missing

        Move certain files from any folder

        .. code:: python

            from onetl.impl import RemoteFile
            from onetl.file import FileMover

            mover = FileMover(target_path="/target", ...)  # no source_path set

            # only absolute paths
            moved_files = mover.run(
                [
                    "/remote/file1.txt",
                    "/any/nested/path/file3.txt",
                ]
            )

            assert moved_files.successful == {
                RemoteFile("/target/file1.txt"),
                RemoteFile("/target/file3.txt"),
                # directory structure is NOT preserved without source_path
            }
            assert not moved_files.failed
            assert not moved_files.skipped
            assert not moved_files.missing
        """

        if files is None and not self.source_path:
            raise ValueError("Neither file list nor `source_path` are passed")

        self._log_options(files)

        # Check everything
        self.connection.check()
        self._check_target_path()
        log_with_indent("")

        if self.source_path:
            self._check_source_path()

        if files is None:
            log.info("|%s| File list is not passed to `run` method", self.__class__.__name__)

            files = self.view_files()

        if not files:
            log.info("|%s| No files to move!", self.__class__.__name__)
            return MoveResult()

        to_move = self._validate_files(files)

        # remove folder only after everything is checked
        if self.options.mode == FileWriteMode.DELETE_ALL:
            self.connection.remove_dir(self.target_path, recursive=True)
            self.connection.create_dir(self.target_path)

        result = self._move_files(to_move)
        self._log_result(result)
        return result

    def view_files(self) -> FileSet[RemoteFile]:
        """
        Get file list in the ``source_path``,
        after ``filter`` and ``limit`` applied (if any).

        Raises
        -------
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

        View files

        .. code:: python

            from onetl.impl import RemoteFile
            from onetl.file import FileMover

            mover = FileMover(source_path="/remote", ...)

            view_files = mover.view_files()

            assert view_files == {
                RemoteFile("/remote/file1.txt"),
                RemoteFile("/remote/file3.txt"),
                RemoteFile("/remote/nested/path/file3.txt"),
            }
        """

        log.info("|%s| Getting files list from path '%s'", self.connection.__class__.__name__, self.source_path)

        self._check_source_path()
        result = FileSet()

        try:
            for root, _dirs, files in self.connection.walk(self.source_path, filters=self.filters, limits=self.limits):
                for file in files:
                    result.append(RemoteFile(path=root / file, stats=file.stats))

        except Exception as e:
            raise RuntimeError(
                f"Couldn't read directory tree from remote dir '{self.source_path}'",
            ) from e

        return result

    def _log_options(self, files: Iterable[str | os.PathLike] | None = None) -> None:  # noqa: WPS213
        entity_boundary_log(msg="FileMover starts")

        connection_class = self.connection.__class__.__name__
        log.info("|%s| -> |%s| Moving files using parameters:", connection_class, connection_class)
        log_with_indent("source_path = %s", f"'{self.source_path}'" if self.source_path else "None")
        log_with_indent("target_path = '%s'", self.target_path)

        if self.filters:
            log_collection("filters", self.filters)
        else:
            log_with_indent("filters = []")

        if self.limits:
            log_collection("limits", self.limits)
        else:
            log_with_indent("limits = []")

        log_options(self.options.dict(by_alias=True))

        if self.options.mode == FileWriteMode.DELETE_ALL:
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

            if self.connection.path_exists(old_file):
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
        total_files = len(to_move)
        files = FileSet(item[0] for item in to_move)

        log.info("|%s| Files to be moved:", self.__class__.__name__)
        log_lines(str(files))
        log_with_indent("")
        log.info("|%s| Starting the move process", self.__class__.__name__)

        result = MoveResult()
        for i, (source_file, target_file) in enumerate(to_move):
            log.info("|%s| Moving file %d of %d", self.__class__.__name__, i + 1, total_files)
            log_with_indent("from = '%s'", source_file)
            log_with_indent("to = '%s'", target_file)

            self._move_file(
                source_file,
                target_file,
                result,
            )

        return result

    def _move_file(  # noqa: WPS231, WPS213
        self,
        source_file: RemotePath,
        target_file: RemotePath,
        result: MoveResult,
    ) -> None:
        if not self.connection.path_exists(source_file):
            log.warning("|%s| Missing file '%s', skipping", self.__class__.__name__, source_file)
            result.missing.add(source_file)
            return

        try:
            replace = False
            if self.connection.path_exists(target_file):
                new_file = self.connection.resolve_file(target_file)

                if self.options.mode == FileWriteMode.ERROR:
                    raise FileExistsError(f"File {path_repr(new_file)} already exists")

                if self.options.mode == FileWriteMode.IGNORE:
                    log.warning(
                        "|%s| File %s already exists, skipping",
                        self.connection.__class__.__name__,
                        path_repr(new_file),
                    )
                    result.skipped.add(source_file)
                    return

                replace = True

            new_file = self.connection.rename_file(source_file, target_file, replace=replace)
            result.successful.add(new_file)

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
            result.failed.add(FailedRemoteFile(path=source_file.path, stats=source_file.stats, exception=e))

    def _log_result(self, result: MoveResult) -> None:
        log_with_indent("")
        log.info("|%s| Move result:", self.__class__.__name__)
        log_lines(str(result))
        entity_boundary_log(msg=f"{self.__class__.__name__} ends", char="-")
