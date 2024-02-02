# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from functools import total_ordering
from string import ascii_lowercase
from typing import Iterator, NamedTuple, Sequence


@total_ordering
class Version(NamedTuple):
    """
    Version representation.

    Examples
    --------

    >>> Version(5, 6, 7)
    Version(major=5, minor=6, patch=7)
    >>> Version(5, 6)
    Version(major=5, minor=6, patch=None)
    >>> Version(5)
    Version(major=5, minor=None, patch=None)

    """

    major: int
    minor: int | None = None
    patch: int | None = None

    def __iter__(self) -> Iterator[int]:
        """
        Iterate over version components which are not ``None``.

        Examples
        --------

        >>> for part in Version(5, 6, 7):
        ...     print(part)
        5
        6
        7

        >>> for part in Version(5, 6):
        ...     print(part)
        5
        6

        >>> for part in Version(5):
        ...     print(part)
        5

        """
        yield self.major
        if self.minor is not None:
            yield self.minor
        if self.patch is not None:
            yield self.patch

    def __len__(self):
        """
        Get number of components set.

        Examples
        --------

        >>> assert len(Version(5, 6, 7)) == 3
        >>> assert len(Version(5, 6)) == 2
        >>> assert len(Version(5)) == 1

        """
        if self.patch is not None:
            return 3
        if self.minor is not None:
            return 2
        return 1

    def __str__(self):
        """
        Get version as string.

        Examples
        --------

        >>> assert str(Version(5, 6, 7)) == "5.6.7"
        >>> assert str(Version(5, 6)) == "5.6"
        >>> assert str(Version(5)) == "5"

        """
        return ".".join(map(str, self))

    def __eq__(self, other):
        """
        Compare versions.

        Examples
        --------

        >>> assert Version(5, 6, 7) == Version(5, 6, 7)

        >>> # Version could be replaced with tuple[int, ...]
        >>> assert Version(5, 6, 7) == (5, 6, 7)
        >>> assert Version(5, 6) == (5, 6)
        >>> assert Version(5) == (5,)

        """
        if not isinstance(other, tuple):
            return NotImplemented

        return tuple(self) == other

    def __gt__(self, other):
        """
        Compare versions.

        Examples
        --------

        >>> assert Version(5, 6, 7) > Version(5, 6, 6)
        >>> assert not Version(5, 6, 7) > Version(5, 6, 7)

        >>> # Version could be replaced with tuple[int, ...]
        >>> assert Version(5, 6, 7) > (5, 6)
        >>> assert not Version(5, 6, 7) > (5, 7)

        >>> assert Version(5, 6) > (5, 5)
        >>> assert not Version(5, 6) > (5, 6)
        >>> assert not Version(5, 6) > (5, 7)

        >>> assert Version(5, 6) > (5,)
        >>> assert not Version(5, 6) > (6,)

        >>> assert Version(5) > (4,)
        >>> assert not Version(5) > (5,)
        >>> assert not Version(5) > (6,)

        """
        if not isinstance(other, tuple):
            return NotImplemented

        return tuple(self) > other

    @classmethod
    def parse(cls, version: int | float | str | Sequence) -> Version:
        """
        Parse input as version object.

        Examples
        --------

        >>> assert Version.parse("5.6.7") == Version(5, 6, 7)
        >>> assert Version.parse("5.6") == Version(5, 6)
        >>> assert Version.parse("5") == Version(5)

        >>> assert Version.parse([5, 6, 7]) == Version(5, 6, 7)
        >>> assert Version.parse([5, 6]) == Version(5, 6)
        >>> assert Version.parse([5]) == Version(5)

        >>> assert Version.parse(5) == Version(5)
        >>> assert Version.parse(5.0) == Version(5, 0)

        """
        if isinstance(version, (int, float)):
            version = str(version)
        if isinstance(version, str):
            version = version.split(".")
        return cls(*map(int, version[:3]))

    def digits(self, items: int) -> Version:
        """
        Return version with exactly N components.

        Raises
        ------
        AssertionError
            There is not enough components

        Examples
        --------

        >>> assert Version(5, 6, 7).digits(3) == Version(5, 6, 7)
        >>> assert Version(5, 6, 7).digits(2) == Version(5, 6)
        >>> assert Version(5, 6, 7).digits(1) == Version(5)
        >>> Version(5, 6).digits(3)
        Traceback (most recent call last):
        AssertionError: Version '5.6' does not match format 'a.b.c'
        >>> Version(5).digits(2)
        Traceback (most recent call last):
        AssertionError: Version '5' does not match format 'a.b'
        """
        if len(self) < items:
            expected = ".".join(ascii_lowercase[:items])
            raise AssertionError(f"Version '{self}' does not match format '{expected}'")
        return self.parse(tuple(self)[:items])
