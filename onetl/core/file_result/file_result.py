from __future__ import annotations

import os
import re
import textwrap
from typing import Iterable, TypeVar

from humanize import naturalsize
from ordered_set import OrderedSet
from pydantic import BaseModel, Field, validator

from onetl.core.file_result.file_set import FileSet
from onetl.exception import FileResultError

GenericPath = TypeVar("GenericPath", bound=os.PathLike)
INDENT = " " * 4


class FileResult(BaseModel):  # noqa: WPS214
    """
    Result of some file manipulation process, e.g. download, upload, etc.

    Container for file paths, divided into certain categories:

    * ``successful`` - successfully handled files
    * ``failed`` - file paths which were handled with some failures
    * ``skipped`` - file paths which were skipped because of some reason
    * ``missing`` - unknown paths which cannot be handled
    """

    class Config:  # noqa: WPS431
        arbitrary_types_allowed = True

    successful: FileSet[GenericPath] = Field(default_factory=FileSet)
    failed: FileSet[GenericPath] = Field(default_factory=FileSet)
    skipped: FileSet[GenericPath] = Field(default_factory=FileSet)
    missing: OrderedSet[GenericPath] = Field(default_factory=OrderedSet)

    @validator("successful", "failed", "skipped")
    def validate_container(cls, value: Iterable[GenericPath]) -> FileSet[GenericPath]:  # noqa: N805
        return FileSet(value)

    @validator("missing")
    def validate_missing_container(cls, value: Iterable[GenericPath]) -> OrderedSet[GenericPath]:  # noqa: N805
        return OrderedSet(value)

    @property
    def successful_count(self) -> int:
        """
        Get number of successful files

        Examples
        --------

        .. code:: python

            from pathlib import Path
            from onet.core import FileResult

            file_result = FileResult(
                successful={Path("/some/file"), Path("/some/another.file")},
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
                successful={Path("/local/file"), Path("/local/another.file")},
                failed={RemoteFile("/remote/file"), RemoteFile("/remote/another.file")},
                skipped={Path("/skipped/file")},
                missing={PurePath("/missing/file")},
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

            from pathlib import Path
            from onet.core import FileResult

            file_result = FileResult(
                successful={Path("/some/file"), Path("/some/another.file")},
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
                successful={Path("/local/file"), Path("/local/another.file")},
                failed={RemoteFile("/remote/file"), RemoteFile("/remote/another.file")},
                skipped={Path("/skipped/file")},
                missing={PurePath("/missing/file")},
            )

            assert file_result.total_size == 10_000_000  # in bytes
        """

        return self.successful_size + self.failed_size + self.skipped_size

    def raise_if_failed(self) -> None:
        """
        Raise exception if there are some files in ``failed`` attribute

        Raises
        ------
        FileResultError

            ``failed`` file set is not empty

        Examples
        --------

        .. code:: python

            from pathlib import Path, PurePath

            from onetl.impl import RemoteFile
            from onet.core import FileResult

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
            # will raise FileResultError('''
            #    Failed 2 file(s) (10MB):
            #        /remote/file1 (1 MB) NotAFileError("'/remote/file1' is not a file")
            #        /remote/file2 (9 MB) FileMissingError("'/remote/file2' does not exist")
            # ''')
        """

        if self.failed:
            raise FileResultError(self._failed_message)

    def raise_if_missing(self) -> None:
        """
        Raise exception if there are some files in ``missing`` attribute

        Raises
        ------
        FileResultError

            ``missing`` file set is not empty

        Examples
        --------

        .. code:: python

            from pathlib import Path, PurePath

            from onetl.impl import RemoteFile
            from onet.core import FileResult

            file_result = FileResult(
                missing={
                    PurePath("/missing/file1"),
                    PurePath("/missing/file2"),
                },
            )

            file_result.raise_if_missing()
            # will raise FileResultError('''
            #    Missing 2 file(s):
            #        /missing/file1
            #        /missing/file2
            # ''')
        """

        if self.missing:
            raise FileResultError(self._missing_message)

    def raise_if_skipped(self) -> None:
        """
        Raise exception if there are some files in ``skipped`` attribute

        Raises
        ------
        FileResultError

            ``skipped`` file set is not empty

        Examples
        --------

        .. code:: python

            from pathlib import Path, PurePath

            from onetl.impl import RemoteFile
            from onet.core import FileResult

            file_result = FileResult(
                skipped={Path("/skipped/file1"), Path("/skipped/file2")},
            )

            file_result.raise_if_skipped()
            # will raise FileResultError('''
            #    Skipped 2 file(s) (15 kB):
            #        /skipped/file1 (10kB)
            #        /skipped/file2 (5 kB)
            # ''')
        """

        if self.skipped:
            raise FileResultError(self._skipped_message)

    def raise_if_no_successful(self) -> None:
        """
        Raise exception if there are no files in ``successful`` attribute

        Raises
        ------
        FileResultError

            ``successful`` file set is empty

        Examples
        --------

        .. code:: python

            from pathlib import Path, PurePath

            from onetl.impl import RemoteFile
            from onet.core import FileResult

            file_result = FileResult()

            file_result.raise_if_no_successful()
            # will raise FileResultError("There are no successful files in the result")
        """

        if not self.successful:
            raise FileResultError("There are no successful files in the result")

    def raise_if_zero_size(self) -> None:
        """
        Raise exception if ``successful`` attribute contains a file with zero size

        Raises
        ------
        FileResultError

            ``successful`` file set contains a file with zero size

        Examples
        --------

        .. code:: python

            from pathlib import Path, PurePath

            from onetl.impl import RemoteFile
            from onet.core import FileResult

            file_result = FileResult(
                successful={
                    Path("/local/empty1.file"),
                    Path("/local/empty2.file"),
                    Path("/local/normal.file"),
                },
            )

            file_result.raise_if_zero_size()
            # will raise FileResultError('''
            #    2 file(s) out of 3 have zero size:
            #        /local/empty1.file
            #        /local/empty2.file
            # ''')
        """

        lines = []
        for file in self.successful:
            if not file.exists() or file.stat().st_size > 0:
                continue

            lines.append(os.fspath(file))

        if not lines:
            return

        lines_str = textwrap.indent(os.linesep.join(lines), INDENT)
        error_message = f"{len(lines)} file(s) out of {self.successful_count} have zero size:{os.linesep}{lines_str}"

        raise FileResultError(error_message)

    def raise_if_empty(self) -> None:
        """
        Raise exception if there are no files in ``successful``, ``failed`` and ``skipped`` attributes

        Raises
        ------
        FileResultError

            ``successful``, ``failed`` and ``skipped`` file sets are empty

        Examples
        --------

        .. code:: python

            from pathlib import Path, PurePath

            from onetl.impl import RemoteFile
            from onet.core import FileResult

            file_result = FileResult()

            file_result.raise_if_empty()
            # will raise FileResultError("There are no files in the result")
        """

        if not self.failed and not self.successful and not self.skipped:
            raise FileResultError("There are no files in the result")

    @property
    def details(self) -> str:
        '''
        Return detailed information about files in the result object

        Examples
        --------

        .. code:: python

            from pathlib import Path, PurePath

            from onetl.impl import RemoteFile
            from onet.core import FileResult

            file_result1 = FileResult(
                successful={Path("/local/file"), Path("/local/another.file")},
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
                skipped={Path("/skipped/file1"), Path("/skipped/file2")},
                missing={PurePath("/missing/file1"), PurePath("/missing/file2")},
            )

            details1 = """
                Total 8 file(s) (10.4 MB)

                Successful 2 file(s) (30.7 kB):
                    /successful1 (10.2 kB)
                    /successful2 (20.5 kB)

                Failed 2 file(s) (10MB):
                    /remote/file1 (1 MB) NotAFileError("'/remote/file1' is not a file")
                    /remote/file2 (9 MB) FileMissingError("'/remote/file2' does not exist")

                Skipped 2 file(s) (15 kB):
                    /skipped/file1 (10kB)
                    /skipped/file2 (5 kB)

                Missing 2 file(s):
                    /missing/file1
                    /missing/file2
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
            result.append(self._total_header)

        result.append(self._successful_message)
        result.append(self._failed_message)
        result.append(self._skipped_message)
        result.append(self._missing_message)

        return (os.linesep * 2).join(result)

    def __str__(self):
        '''
        Return short summary about files in the result object

        Examples
        --------

        .. code:: python

            from pathlib import Path, PurePath

            from onetl.impl import RemoteFile
            from onet.core import FileResult

            file_result1 = FileResult(
                successful={Path("/local/file"), Path("/local/another.file")},
                failed={RemoteFile("/remote/file"), RemoteFile("/remote/another.file")},
                skipped={Path("/skipped/file")},
                missing={PurePath("/missing/file")},
            )

            result = """
                Total 6 file(s) (10.4 MB)

                Successful 2 file(s) (30.7 kB)

                Failed 2 file(s) (10MB)

                Skipped 2 file(s) (15 kB)

                Missing 2 file(s)
            """

            assert str(file_result1) == result

            file_result2 = FileResult()
            assert str(file_result1) == "No files"
        '''

        return self._total_message

    @property
    def _total_header(self) -> str:
        if self.successful or self.failed or self.missing or self.skipped:
            return f"Total {self.total_count} file(s) ({naturalsize(self.total_size)})"

        return "No files"

    @property
    def _successful_header(self) -> str:
        if not self.successful:
            return "No successful files"

        return f"Successful {self.successful_count} file(s) ({naturalsize(self.successful_size)})"

    @property
    def _failed_header(self) -> str:
        if not self.failed:
            return "No failed files"

        return f"Failed {self.failed_count} file(s) ({naturalsize(self.failed_size)})"

    @property
    def _skipped_header(self) -> str:
        if not self.skipped:
            return "No skipped files"

        return f"Skipped {self.skipped_count} file(s) ({naturalsize(self.skipped_size)})"

    @property
    def _missing_header(self) -> str:
        if not self.missing:
            return "No missing files"

        return f"Missing {self.missing_count} file(s)"

    @property
    def _successful_message(self) -> str:
        if not self.successful:
            return self._successful_header

        lines = []
        for file in self.successful:
            size = naturalsize(file.stat().st_size) if file.exists() else "? Bytes"

            lines.append(f"{os.fspath(file)} ({size})")

        if self.successful_count > 1:
            header = f"{self._successful_header}:{os.linesep}{INDENT}"
        else:
            header = "Successful: "

        lines_str = textwrap.indent(os.linesep.join(lines), INDENT).strip()
        return header + lines_str

    @property
    def _failed_message(self) -> str:
        if not self.failed:
            return self._failed_header

        lines = []
        for file in self.failed:
            size = naturalsize(file.stat().st_size) if file.exists() else "? Bytes"
            prefix = f"{os.fspath(file)} ({size})"

            exception_formatted = ""
            if hasattr(file, "exception"):
                exception = re.sub(r"(\\r)?\\n", os.linesep, repr(file.exception))
                exception_formatted = " " + textwrap.indent(exception, " " * (len(prefix) + 2)).strip()
            lines.append(prefix + exception_formatted)

        if self.failed_count > 1:
            header = f"{self._failed_header}:{os.linesep}{INDENT}"
        else:
            header = "Failed: "

        lines_str = textwrap.indent(os.linesep.join(lines), INDENT).strip()
        return header + lines_str

    @property
    def _skipped_message(self) -> str:
        if not self.skipped:
            return self._skipped_header

        lines = []
        for file in self.skipped:
            size = naturalsize(file.stat().st_size) if file.exists() else "? Bytes"

            lines.append(f"{os.fspath(file)} ({size})")

        if self.skipped_count > 1:
            header = f"{self._skipped_header}:{os.linesep}{INDENT}"
        else:
            header = "Skipped: "

        lines_str = textwrap.indent(os.linesep.join(lines), INDENT).strip()
        return header + lines_str

    @property
    def _missing_message(self) -> str:
        if not self.missing:
            return self._missing_header

        lines = [os.fspath(file) for file in self.missing]

        if self.missing_count > 1:
            header = f"{self._missing_header}:{os.linesep}{INDENT}"
        else:
            header = "Missing: "

        lines_str = textwrap.indent(os.linesep.join(lines), INDENT).strip()
        return header + lines_str

    @property
    def _total_message(self) -> str:
        result = [self._total_header]

        if self.successful:
            result.append(self._successful_header)

        if self.failed:
            result.append(self._failed_header)

        if self.skipped:
            result.append(self._skipped_header)

        if self.missing:
            result.append(self._missing_header)

        return os.linesep.join(result)
