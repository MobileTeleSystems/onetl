import os
import textwrap
from typing import Generic, TypeVar

from humanize import naturalsize
from ordered_set import OrderedSet

from onetl.base import PathProtocol, SizedPathProtocol
from onetl.impl import humanize_path

T = TypeVar("T", bound=PathProtocol)
INDENT = " " * 4


class FileSet(OrderedSet[T], Generic[T]):  # noqa: WPS600
    """
    Ordered set of pathlib-like objects.

    It has all the methods of generic set (e.g. ``add``, ``difference``, ``intersection``),
    as well as list (e.g. ``append``, ``index``, ``[]``).

    It also has a ``total_size`` helper method.
    """

    @property
    def total_size(self) -> int:
        """
        Get total size (in bytes) of files in the set

        Examples
        --------

        .. code:: python

            from onetl.impl import LocalPath
            from onet.core import FileSet

            file_set = FileSet({LocalPath("/some/file"), LocalPath("/some/another.file")})

            assert path_set.total_size == 1_000_000  # in bytes
        """

        return sum(file.stat().st_size for file in self if isinstance(file, SizedPathProtocol) and file.exists())

    @property
    def summary(self) -> str:
        """
        Return summary about files in the set

        Examples
        --------

        .. code:: python

            from onetl.impl import LocalFile
            from onet.core import FileSet

            path_set1 = FileSet(
                [
                    LocalFile("/local/file"),
                    LocalFile("/local/another.file"),
                ]
            )

            assert path_set1.summary == "2 files (30.7 kB)"

            assert FileSet().summary == "No files"
        """

        if not self:
            return "No files"

        file_number_str = f"{len(self)} files" if len(self) > 1 else "1 file"

        return f"{file_number_str} ({naturalsize(self.total_size)})"

    @property
    def details(self) -> str:
        '''
        Return detailed information about files in the set

        Examples
        --------

        .. code:: python

            from onetl.impl import LocalFile
            from onet.core import FileSet

            path_set1 = FileSet(
                [
                    LocalFile("/local/file"),
                    LocalFile("/local/another.file"),
                ]
            )

            details1 = """
                2 files (30.7 kB):
                    /local/file (10.2 kB)
                    /local/another.file (20.5 kB)
            """

            assert path_set1.details == details1

            assert FileSet().details == "No files"
        '''

        if not self:
            return self.summary

        lines = [humanize_path(file) for file in self]  # type: ignore[arg-type]

        summary = f"{self.summary}:{os.linesep}{INDENT}"

        lines_str = textwrap.indent(os.linesep.join(lines), INDENT).strip()
        return summary + lines_str

    def __str__(self) -> str:
        """Same as :obj:`onetl.core.file_result.file_set.FileSet.details`"""
        return self.details
