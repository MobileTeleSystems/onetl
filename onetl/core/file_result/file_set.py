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

import os
import textwrap
from typing import Generic, TypeVar

from humanize import naturalsize
from ordered_set import OrderedSet

from onetl.base import PathProtocol, PathWithStatsProtocol
from onetl.exception import EmptyFilesError, ZeroFileSizeError
from onetl.impl import path_repr

T = TypeVar("T", bound=PathProtocol)
INDENT = " " * 4


class FileSet(OrderedSet[T], Generic[T]):
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

        return sum(
            file.stat().st_size or 0 for file in self if isinstance(file, PathWithStatsProtocol) and file.exists()
        )

    def raise_if_empty(self) -> None:
        """
        Raise exception if there are no files in the set

        Raises
        ------
        EmptyFilesError

            File set is empty

        Examples
        --------

        .. code:: python

            from onet.core import FileSet

            file_set = FileSet()

            file_set.raise_if_empty()
            # will raise EmptyFilesError("There are no files in the set")
        """

        if not self:
            raise EmptyFilesError("There are no files in the set")

    def raise_if_contains_zero_size(self) -> None:
        """
        Raise exception if file set contains a file with zero size

        Raises
        ------
        ZeroFileSizeError

            File set contains a file with zero size

        Examples
        --------

        .. code:: python

            from onetl.impl import RemoteFile, LocalPath
            from onet.core import FileSet

            file_set = FileSet(
                LocalPath("/local/empty1.file"),
                LocalPath("/local/empty2.file"),
                LocalPath("/local/normal.file"),
            )

            file_set.raise_if_contains_zero_size()
            # will raise ZeroFileSizeError('''
            #    2 files out of 3 have zero size:
            #        '/local/empty1.file'
            #        '/local/empty2.file'
            # ''')
        """

        lines = []
        for file in self:
            if not file.exists() or file.stat().st_size > 0:
                continue

            lines.append(repr(os.fspath(file)))

        if not lines:
            return

        lines_str = textwrap.indent(os.linesep.join(lines), INDENT)
        file_number_str = f"{len(lines)} files" if len(lines) > 1 else "1 file"
        error_message = f"{file_number_str} out of {len(self)} have zero size:{os.linesep}{lines_str}"

        raise ZeroFileSizeError(error_message)

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

        return f"{file_number_str} (size='{naturalsize(self.total_size)}')"

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
                    '/local/file' (10.2 kB)
                    '/local/another.file' (20.5 kB)
            """

            assert path_set1.details == details1

            assert FileSet().details == "No files"
        '''

        if not self:
            return self.summary

        lines = [path_repr(file, with_mode=False, with_kind=False, with_owner=False, with_mtime=False) for file in self]

        summary = f"{self.summary}:{os.linesep}{INDENT}"

        lines_str = textwrap.indent(os.linesep.join(lines), INDENT).strip()
        return summary + lines_str

    def __str__(self) -> str:
        """Same as :obj:`onetl.core.file_set.file_set.FileSet.details`"""
        return self.details
