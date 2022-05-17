from __future__ import annotations

import os
from typing import Iterable, Set, TypeVar

from humanize import naturalsize
from pydantic import BaseModel, Field, validator

from onetl.core.file_result.file_set import FileSet

GenericPath = TypeVar("GenericPath", bound=os.PathLike)


class FileResult(BaseModel):
    """
    Result of some file manipulation process, e.g. download, upload, etc.

    Container for file paths, divided into certain categories:

    * ``success`` - successfully handled files
    * ``failed`` - file paths which were handled with some failures
    * ``skipped`` - file paths which were skipped because of some reason
    * ``missing`` - unknown paths which cannot be handled
    """

    class Config:  # noqa: WPS431
        arbitrary_types_allowed = True

    success: FileSet[GenericPath] = Field(default_factory=FileSet)
    failed: FileSet[GenericPath] = Field(default_factory=FileSet)
    skipped: FileSet[GenericPath] = Field(default_factory=FileSet)
    missing: Set[GenericPath] = Field(default_factory=set)

    @validator("success", "failed", "skipped")
    def validate_container(cls, value: Iterable[GenericPath]) -> FileSet[GenericPath]:  # noqa: N805
        return FileSet(value)

    @validator("missing")
    def validate_missing(cls, value: Iterable[GenericPath]) -> set[GenericPath]:  # noqa: N805
        return set(value)

    @property
    def success_count(self) -> int:
        """
        Get number of success files

        Examples
        --------

        .. code:: python

            from pathlib import Path
            from onet.core import FileResult

            file_result = FileResult(
                success={Path("/some/file"), Path("/some/another.file")},
            )

            assert file_result.success_count == 2
        """

        return len(self.success)

    @property
    def failed_count(self) -> int:
        """
        Get number of failed files

        Examples
        --------

        .. code:: python

            from onetl.impl import RemoteFile
            from onet.core import FileResult

            file_result = FileResult(
                failed={RemoteFile("/some/file"), RemoteFile("/some/another.file")},
            )

            assert file_result.failed_count == 2
        """

        return len(self.failed)

    @property
    def skipped_count(self) -> int:
        """
        Get number of skipped files

        Examples
        --------

        .. code:: python

            from pathlib import Path
            from onet.core import FileResult

            file_result = FileResult(
                skipped={Path("/some/file"), Path("/some/another.file")},
            )

            assert file_result.skipped_count == 2
        """

        return len(self.skipped)

    @property
    def missing_count(self) -> int:
        """
        Get number of missing files

        Examples
        --------

        .. code:: python

            from pathlib import PurePath
            from onet.core import FileResult

            file_result = FileResult(
                missing={PurePath("/some/file"), PurePath("/some/another.file")},
            )

            assert file_result.missing_count == 2
        """

        return len(self.missing)

    @property
    def total_count(self) -> int:
        """
        Get total number of all files

        Examples
        --------

        .. code:: python

            from pathlib import Path, PurePath

            from onetl.impl import RemoteFile
            from onet.core import FileResult

            file_result = FileResult(
                success={Path("/local/file"), Path("/local/another.file")},
                failed={RemoteFile("/remote/file"), RemoteFile("/remote/another.file")},
                skipped={Path("/skipped/file")},
                missing={PurePath("/missing/file")},
            )

            assert file_result.total_count == 6
        """

        return self.success_count + self.failed_count + self.missing_count + self.skipped_count

    @property
    def success_size(self) -> int:
        """
        Get size (in bytes) of success files

        Examples
        --------

        .. code:: python

            from pathlib import Path
            from onet.core import FileResult

            file_result = FileResult(
                success={Path("/some/file"), Path("/some/another.file")},
            )

            assert file_result.success_size == 1_000_000  # in bytes
        """

        return self.success.total_size

    @property
    def failed_size(self) -> int:
        """
        Get size (in bytes) of failed files

        Examples
        --------

        .. code:: python

            from onetl.impl import RemoteFile
            from onet.core import FileResult

            file_result = FileResult(
                failed={RemoteFile("/some/file"), RemoteFile("/some/another.file")},
            )

            assert file_result.failed_size == 1_000_000  # in bytes
        """

        return self.failed.total_size

    @property
    def skipped_size(self) -> int:
        """
        Get size (in bytes) of skipped files

        Examples
        --------

        .. code:: python

            from pathlib import Path
            from onet.core import FileResult

            file_result = FileResult(
                skipped={Path("/some/file"), Path("/some/another.file")},
            )

            assert file_result.skipped_size == 1_000_000  # in bytes
        """

        return self.skipped.total_size

    @property
    def total_size(self) -> int:
        """
        Get total size (in bytes) of all files

        Examples
        --------

        .. code:: python

            from pathlib import Path, PurePath

            from onetl.impl import RemoteFile
            from onet.core import FileResult

            file_result = FileResult(
                success={Path("/local/file"), Path("/local/another.file")},
                failed={RemoteFile("/remote/file"), RemoteFile("/remote/another.file")},
                skipped={Path("/skipped/file")},
                missing={PurePath("/missing/file")},
            )

            assert file_result.total_size == 10_000_000  # in bytes
        """

        return self.success_size + self.failed_size + self.skipped_size

    def __str__(self):
        result = []

        if self.success_count:
            if self.failed_count or self.missing_count or self.skipped_count:
                result.append(f"Total: {self.total_count} file(s) {naturalsize(self.total_size)}")

            result.append(f"Successful: {self.success_count} file(s) {naturalsize(self.success_size)}")

        if self.failed_count:
            result.append(f"Failed: {self.failed_count} file(s) {naturalsize(self.failed_size)}")

        if self.skipped_count:
            result.append(f"Skipped: {self.skipped_count} file(s) {naturalsize(self.skipped_size)}")

        if self.missing_count:
            result.append(f"Missing: {self.missing_count} file(s)")

        if not result:
            return "No files"

        return os.linesep.join(result)
