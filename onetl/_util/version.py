# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import re
from functools import total_ordering


@total_ordering
class Version:
    """
    Version representation.

    Examples
    --------

    >>> Version("12.3.1")
    Version('12.3.1')
    >>> Version("12.3")
    Version('12.3')
    >>> Version("12.3.4.5")
    Version('12.3.4.5')
    >>> Version("12.3.4-patch5")
    Version('12.3.4-patch5')

    """

    def __init__(self, version: str):
        self._raw_str = version
        self._raw_parts = re.split("[.-]", version)
        self._numeric_parts = [int(part) for part in self._raw_parts if part.isdigit()]

    @property
    def major(self) -> int:
        """
        Return the major version component.

        Examples
        --------

        >>> Version("5.6.7").major
        5
        """
        return self._numeric_parts[0] if self._numeric_parts else 0

    @property
    def minor(self) -> int:
        """
        Return the minor version component.

        Examples
        --------

        >>> Version("5.6.7").minor
        6
        >>> Version("5").minor
        0
        """
        return self._numeric_parts[1] if len(self._numeric_parts) > 1 else 0

    @property
    def patch(self) -> int:
        """
        Return the patch version component.

        Examples
        --------

        >>> Version("5.6.7").patch
        7
        >>> Version("5.6").patch
        0
        """
        return self._numeric_parts[2] if len(self._numeric_parts) > 2 else 0

    @property
    def raw_parts(self) -> list[str]:
        """
        Returns the parts of the version string as a list of substrings split by '.' or '-'.

        Examples
        --------
        >>> Version("1.2.3-alpha").raw_parts
        ['1', '2', '3', 'alpha']

        """
        return self._raw_parts

    def __getitem__(self, item):
        """
        Allows direct access to the numeric parts of the version by index.

        Examples
        --------
        >>> Version("1.2.3")[0]
        1
        >>> Version("1.2.3")[1]
        2
        >>> Version("1.2.3")[2]
        3
        >>> Version("1.2.3-alpha")[3]
        Traceback (most recent call last):
            ...
        IndexError: list index out of range
        """
        return self._numeric_parts[item]

    def __len__(self):
        """
        Get number of components set.

        Examples
        --------

        >>> len(Version("5.6.7"))
        3
        >>> len(Version("5.6"))
        2
        >>> len(Version("5"))
        1

        """
        return len(self._numeric_parts)

    def __repr__(self):
        return f"Version('{self._raw_str}')"

    def __str__(self):
        """
        Return a string representation of the version.

        Examples
        --------

        >>> str(Version("5.6.7"))
        '5.6.7'
        >>> str(Version("5.6"))
        '5.6'
        >>> str(Version("5.6.7.8"))
        '5.6.7.8'
        >>> str(Version("5.6.7-patch8"))
        '5.6.7-patch8'

        """
        return self._raw_str

    def __eq__(self, other):
        """
        Compare two versions for equality.

        Examples
        --------

        >>> Version("5.6.7") == Version("5.6.7")
        True
        >>> Version("5.6.7") == Version("5.6.8")
        False
        """
        if not isinstance(other, Version):
            return NotImplemented
        return self._numeric_parts == other._numeric_parts

    def __lt__(self, other: Version):
        """
        Compare two versions using less than.

        Examples
        --------

        >>> Version("5.6.7") < Version("5.6.8")
        True
        >>> Version("5.6.9") < Version("5.6.8")
        False
        """
        if not isinstance(other, Version):
            return NotImplemented
        return self._numeric_parts < other._numeric_parts

    def min_digits(self, num_parts: int) -> Version:
        """
        Ensure the version has at least a specified number of numeric components.

        Raises
        ------
        ValueError
            There is not enough components

        Examples
        --------
        >>> Version("5.6.7").min_digits(3)
        Version('5.6.7')
        >>> Version("5.6.7").min_digits(2)
        Version('5.6.7')
        >>> Version("5.6").min_digits(3)
        Traceback (most recent call last):
            ...
        ValueError: Version '5.6' does not have enough numeric components for requested format (expected at least 3).
        """
        if len(self._numeric_parts) < num_parts:
            raise ValueError(
                f"Version '{self}' does not have enough numeric components for requested format (expected at least {num_parts}).",
            )
        return self

    def format(self, format_string: str) -> str:
        """
        Format the version using a custom format string.

        Examples
        --------
        >>> v = Version("5.6.7")
        >>> v.format("{major}.{minor}.{patch}")
        '5.6.7'
        >>> v.format("{0}.{1}.{2}")
        '5.6.7'
        >>> v.format("{0}.{1}.{2} - Complete Version")
        '5.6.7 - Complete Version'
        >>> v = Version("12.3.4-patch5")
        >>> v.format("{major}.{minor}.{patch}")
        '12.3.4'
        """
        return format_string.format(
            *self._numeric_parts,
            major=self.major,
            minor=self.minor,
            patch=self.patch,
        )
