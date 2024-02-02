# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
from typing import Iterable

from humanize import naturalsize
from pydantic import Field, validator

from onetl.base import PurePathProtocol
from onetl.exception import (
    EmptyFilesError,
    FailedFilesError,
    MissingFilesError,
    SkippedFilesError,
)
from onetl.file.file_set import FileSet
from onetl.impl import BaseModel

INDENT = " " * 4


class FileResult(BaseModel):
    """
    Result of some file manipulation process, e.g. download, upload, etc.

    Container for file paths, divided into certain categories:

    * :obj`successful`
    * :obj`failed`
    * :obj`skipped`
    * :obj`missing`
    """

    successful: FileSet[PurePathProtocol] = Field(default_factory=FileSet)
    "Successfully handled files"

    failed: FileSet[PurePathProtocol] = Field(default_factory=FileSet)
    "File paths which were handled with some failures"

    skipped: FileSet[PurePathProtocol] = Field(default_factory=FileSet)
    "File paths which were skipped because of some reason"

    missing: FileSet[PurePathProtocol] = Field(default_factory=FileSet)
    "Unknown paths which cannot be handled"

    @validator("successful", "failed", "skipped", "missing")
    def validate_container(cls, value: Iterable[PurePathProtocol]) -> FileSet[PurePathProtocol]:
        return FileSet(value)

    @property
    def successful_count(self) -> int:
        """
        Get number of successful files

        Examples
        --------

        .. code:: python

            from onetl.impl import LocalPath
            from onet.file.file_result import FileResult

            file_result = FileResult(
                successful={LocalPath("/some/file"), LocalPath("/some/another.file")},
            )

            assert file_result.successful_count == 2
        """

        return len(self.successful)

    @property
    def failed_count(self) -> int:
        """
        Get number of failed files

        Examples
        --------

        .. code:: python

            from onetl.impl import RemoteFile
            from onet.file.file_result import FileResult

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

            from onetl.impl import LocalPath
            from onet.file.file_result import FileResult

            file_result = FileResult(
                skipped={LocalPath("/some/file"), LocalPath("/some/another.file")},
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

            from onetl.impl import LocalPath
            from onet.file.file_result import FileResult

            file_result = FileResult(
                missing={LocalPath("/some/file"), LocalPath("/some/another.file")},
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

            from onetl.impl import RemoteFile, LocalPath
            from onet.file.file_result import FileResult

            file_result = FileResult(
                successful={LocalPath("/local/file"), LocalPath("/local/another.file")},
                failed={RemoteFile("/remote/file"), RemoteFile("/remote/another.file")},
                skipped={LocalPath("/skipped/file")},
                missing={LocalPath("/missing/file")},
            )

            assert file_result.total_count == 6
        """

        return self.successful_count + self.failed_count + self.missing_count + self.skipped_count

    @property
    def successful_size(self) -> int:
        """
        Get size (in bytes) of successful files

        Examples
        --------

        .. code:: python

            from onetl.impl import LocalPath
            from onet.file.file_result import FileResult

            file_result = FileResult(
                successful={LocalPath("/some/file"), LocalPath("/some/another.file")},
            )

            assert file_result.successful_size == 1_000_000  # in bytes
        """

        return self.successful.total_size

    @property
    def failed_size(self) -> int:
        """
        Get size (in bytes) of failed files

        Examples
        --------

        .. code:: python

            from onetl.impl import RemoteFile
            from onet.file.file_result import FileResult

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

            from onetl.impl import LocalPath
            from onet.file.file_result import FileResult

            file_result = FileResult(
                skipped={LocalPath("/some/file"), LocalPath("/some/another.file")},
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

            from onetl.impl import RemoteFile, LocalPath
            from onet.file.file_result import FileResult

            file_result = FileResult(
                successful={LocalPath("/local/file"), LocalPath("/local/another.file")},
                failed={RemoteFile("/remote/file"), RemoteFile("/remote/another.file")},
                skipped={LocalPath("/skipped/file")},
                missing={LocalPath("/missing/file")},
            )

            assert file_result.total_size == 10_000_000  # in bytes
        """

        return self.successful_size + self.failed_size + self.skipped_size

    def raise_if_failed(self) -> None:
        """
        Raise exception if there are some files in ``failed`` attribute

        Raises
        ------
        FailedFilesError

            ``failed`` file set is not empty

        Examples
        --------

        .. code:: python

            from onetl.impl import RemoteFile, LocalPath
            from onet.file.file_result import FileResult

            files_with_exception = [
                FailedRemoteFile(
                    path="/remote/file1",
                    exception=NotAFileError("'/remote/file1' is not a file"),
                ),
                FailedRemoteFile(
                    path="/remote/file2",
                    exception=FileMissingError("'/remote/file2' does not exist"),
                ),
            ]

            file_result = FileResult(failed=files_with_exception)

            file_result.raise_if_failed()
            # will raise FailedFilesError('''
            #    Failed 2 files (10MB):
            #        '/remote/file1' (1 MB)
            #           NotAFileError("'/remote/file1' is not a file")
            #
            #        '/remote/file2' (9 MB)
            #           FileMissingError("'/remote/file2' does not exist")
            # ''')
        """

        if self.failed:
            raise FailedFilesError(self._failed_message)

    def raise_if_missing(self) -> None:
        """
        Raise exception if there are some files in ``missing`` attribute

        Raises
        ------
        MissingFilesError

            ``missing`` file set is not empty

        Examples
        --------

        .. code:: python

            from onetl.impl import RemoteFile, LocalPath
            from onet.file.file_result import FileResult

            file_result = FileResult(
                missing={
                    LocalPath("/missing/file1"),
                    LocalPath("/missing/file2"),
                },
            )

            file_result.raise_if_missing()
            # will raise MissingFilesError('''
            #    Missing 2 files:
            #        '/missing/file1'
            #        '/missing/file2'
            # ''')
        """

        if self.missing:
            raise MissingFilesError(self._missing_message)

    def raise_if_skipped(self) -> None:
        """
        Raise exception if there are some files in ``skipped`` attribute

        Raises
        ------
        SkippedFilesError

            ``skipped`` file set is not empty

        Examples
        --------

        .. code:: python

            from onetl.impl import RemoteFile, LocalPath
            from onet.file.file_result import FileResult

            file_result = FileResult(
                skipped={LocalPath("/skipped/file1"), LocalPath("/skipped/file2")},
            )

            file_result.raise_if_skipped()
            # will raise SkippedFilesError('''
            #    Skipped 2 files (15 kB):
            #        '/skipped/file1' (10kB)
            #        '/skipped/file2' (5 kB)
            # ''')
        """

        if self.skipped:
            raise SkippedFilesError(self._skipped_message)

    def raise_if_contains_zero_size(self) -> None:
        """
        Raise exception if ``successful`` attribute contains a file with zero size

        Raises
        ------
        ZeroFileSizeError

            ``successful`` file set contains a file with zero size

        Examples
        --------

        .. code:: python

            from onetl.impl import RemoteFile, LocalPath
            from onet.file.file_result import FileResult

            file_result = FileResult(
                successful={
                    LocalPath("/local/empty1.file"),
                    LocalPath("/local/empty2.file"),
                    LocalPath("/local/normal.file"),
                },
            )

            file_result.raise_if_contains_zero_size()
            # will raise ZeroFileSizeError('''
            #    2 files out of 3 have zero size:
            #        '/local/empty1.file'
            #        '/local/empty2.file'
            # ''')
        """

        self.successful.raise_if_contains_zero_size()

    @property
    def is_empty(self) -> bool:
        """
        Returns ``True`` if there are no files in ``successful``, ``failed`` and ``skipped`` attributes

        Examples
        --------

        .. code:: python

            from onetl.impl import LocalPath
            from onet.file.file_result import FileResult

            file_result1 = FileResult()
            assert file_result1.is_empty

            file_result2 = FileResult(
                successful={LocalPath("/local/file"), LocalPath("/local/another.file")},
            )
            assert not file_result2.is_empty
        """

        return not self.failed and not self.successful and not self.skipped

    def raise_if_empty(self) -> None:
        """
        Raise exception if there are no files in ``successful``, ``failed`` and ``skipped`` attributes

        Raises
        ------
        EmptyFilesError

            ``successful``, ``failed`` and ``skipped`` file sets are empty

        Examples
        --------

        .. code:: python

            from onetl.impl import RemoteFile, LocalPath
            from onet.file.file_result import FileResult

            file_result = FileResult()

            file_result.raise_if_empty()
            # will raise EmptyFilesError("There are no files in the result")
        """

        if self.is_empty:
            raise EmptyFilesError("There are no files in the result")

    @property
    def details(self) -> str:
        '''
        Return detailed information about files in the result object

        Examples
        --------

        .. code:: python

            from onetl.impl import RemoteFile, LocalPath
            from onet.file.file_result import FileResult

            file_result1 = FileResult(
                successful={LocalPath("/local/file"), LocalPath("/local/another.file")},
                failed={
                    FailedRemoteFile(
                        path="/remote/file1",
                        exception=NotAFileError("'/remote/file1' is not a file"),
                    ),
                    FailedRemoteFile(
                        path="/remote/file2",
                        exception=FileMissingError("'/remote/file2' does not exist"),
                    ),
                },
                skipped={LocalPath("/skipped/file1"), LocalPath("/skipped/file2")},
                missing={LocalPath("/missing/file1"), LocalPath("/missing/file2")},
            )

            details1 = """
                Total: 8 files (10.4 MB)

                Successful 2 files (30.7 kB):
                    '/successful1' (10.2 kB)
                    '/successful2' (20.5 kB)

                Failed 2 files (10MB):
                    '/remote/file1' (1 MB)
                        NotAFileError("'/remote/file1' is not a file")

                    '/remote/file2' (9 MB)
                        FileMissingError("'/remote/file2' does not exist")

                Skipped 2 files (15 kB):
                    '/skipped/file1' (10kB)
                    '/skipped/file2' (5 kB)

                Missing 2 files:
                    '/missing/file1'
                    '/missing/file2'
            """

            assert file_result1.details == details1

            file_result2 = FileResult()
            details2 = """
                No successful files

                No failed files

                No skipped files

                No missing files
            """

            assert file_result2.details == details2
        '''

        result = []

        if self.successful or self.failed or self.missing or self.skipped:
            result.append(self._total_summary)

        result.append(self._successful_message)
        result.append(self._failed_message)
        result.append(self._skipped_message)
        result.append(self._missing_message)

        return (os.linesep * 2).join(result)

    @property
    def summary(self) -> str:
        '''
        Return short summary about files in the result object

        Examples
        --------

        .. code:: python

            from onetl.impl import RemoteFile, LocalPath
            from onet.file.file_result import FileResult

            file_result1 = FileResult(
                successful={LocalPath("/local/file"), LocalPath("/local/another.file")},
                failed={RemoteFile("/remote/file"), RemoteFile("/remote/another.file")},
                skipped={LocalPath("/skipped/file")},
                missing={LocalPath("/missing/file")},
            )

            result = """
                Total: 8 files (10.4 MB)

                Successful: 2 files (30.7 kB)

                Failed: 2 files (10MB)

                Skipped: 2 files (15 kB)

                Missing: 2 files
            """

            assert file_result1.summary == result

            file_result2 = FileResult()
            assert file_result1.summary == "No files"
        '''
        return self._total_message

    def __str__(self):
        """Same as :obj:`onetl.file.file_result.FileResult.details`"""
        return self.details

    @property
    def _total_summary(self) -> str:
        if self.successful or self.failed or self.missing or self.skipped:
            file_number_str = f"{self.total_count} files" if self.total_count > 1 else "1 file"
            return f"Total: {file_number_str} (size='{naturalsize(self.total_size)}')"

        return "No files"

    @property
    def _successful_summary(self) -> str:
        if not self.successful:
            return "No successful files"

        return "Successful: " + self.successful.summary

    @property
    def _successful_message(self) -> str:
        if not self.successful:
            return self._successful_summary

        return "Successful " + self.successful.details

    @property
    def _failed_summary(self) -> str:
        if not self.failed:
            return "No failed files"

        return "Failed: " + self.failed.summary

    @property
    def _failed_message(self) -> str:
        if not self.failed:
            return self._failed_summary

        return "Failed " + self.failed.details

    @property
    def _skipped_summary(self) -> str:
        if not self.skipped:
            return "No skipped files"

        return "Skipped: " + self.skipped.summary

    @property
    def _skipped_message(self) -> str:
        if not self.skipped:
            return self._skipped_summary

        return "Skipped " + self.skipped.details

    @property
    def _missing_summary(self) -> str:
        if not self.missing:
            return "No missing files"

        return "Missing: " + self.missing.summary.replace(" (size='0 Bytes')", "")

    @property
    def _missing_message(self) -> str:
        if not self.missing:
            return self._missing_summary

        return "Missing " + self.missing.details.replace(" (size='0 Bytes')", "")

    @property
    def _total_message(self) -> str:
        result = [self._total_summary]

        if self.successful:
            result.append(self._successful_summary)

        if self.failed:
            result.append(self._failed_summary)

        if self.skipped:
            result.append(self._skipped_summary)

        if self.missing:
            result.append(self._missing_summary)

        return os.linesep.join(result)
