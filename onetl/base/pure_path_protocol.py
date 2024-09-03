# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Sequence, TypeVar

from typing_extensions import Protocol, runtime_checkable

T = TypeVar("T", bound="PurePathProtocol", covariant=True)


@runtime_checkable
class PurePathProtocol(Protocol[T]):
    """
    Generic protocol for :obj:`pathlib.PurePath` like objects.

    Includes only minimal set of methods which allow to get path items, like parent, name, etc
    """

    def __fspath__(self) -> str:
        """
        Get string representation of path
        """

    def __eq__(self, other) -> bool:
        """
        Check if two paths are equal
        """

    def __hash__(self) -> int:
        """
        Get hash value for path
        """

    def __truediv__(self, key) -> T:
        """
        Add items to path
        """

    def __rtruediv__(self, key) -> T:
        """
        Add items to path
        """

    @property
    def name(self) -> str:
        """
        Get path name
        """

    @property
    def parent(self) -> T:
        """
        Get parent path
        """

    @property
    def parents(self) -> Sequence[T]:
        """
        Get parent paths
        """

    @property
    def parts(self) -> Sequence[str]:
        """
        Get path parts
        """

    def is_absolute(self) -> bool:
        """
        Checks if this path is absolute
        """

    def match(self, path_pattern) -> bool:
        """
        Checks if path matches a glob pattern
        """

    def relative_to(self, *other) -> T:
        """
        Return the relative path to another path
        """

    def as_posix(self) -> str:
        """
        Get POSIX representation of path
        """

    def joinpath(self, *args) -> T:
        """
        Add items to path
        """
