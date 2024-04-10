# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
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

    def __init__(self, version):
        if isinstance(version, Version):
            self._raw_str = version._raw_str
            self._raw_parts = version._raw_parts.copy()
            self._numeric_parts = version._numeric_parts.copy()
        elif isinstance(version, str):
            self._raw_str = version
            self._raw_parts = re.split("[.-]", version)
            self._numeric_parts = []

            # process parts to keep leading zeros in minor or patch levels (like '00' in '12.23.00.34')
            for part in self._raw_parts:
                clean_part = "".join(filter(str.isdigit, part)) or "0"
                self._numeric_parts.append(int(clean_part))

    @property
    def major(self) -> int:
        """
        Return the major version component.

        Examples
        --------

        >>> Version("5.6.7").major
        5
        """
        return self._numeric_parts[0] if self._numeric_parts else None

    @property
    def minor(self) -> int:
        """
        Return the minor version component.

        Examples
        --------

        >>> Version("5.6.7").minor
        6
        """
        return self._numeric_parts[1] if len(self._numeric_parts) > 1 else None

    @property
    def patch(self) -> int:
        """
        Return the patch version component.

        Examples
        --------

        >>> Version("5.6.7").patch
        7
        """
        return self._numeric_parts[2] if len(self._numeric_parts) > 2 else None

    @property
    def raw_parts(self) -> list[str]:
        return self._raw_parts

    def __getitem__(self, item):
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

        >>> str(Version("5.6.7-patch4"))
        '5.6.7-patch4'

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

    def __lt__(self, other):
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

    def digits(self, num_parts: int) -> Version:
        """
        Return a version object truncated to the first 'num_parts' components.

        Raises
        ------
        ValueError
            There is not enough components

        Examples
        --------

        >>> Version("5.6.7.8").digits(3)
        Version('5.6.7')
        >>> Version("5.6").digits(3)
        Traceback (most recent call last):
            ...
        ValueError: Version '5.6' does not have enough components for requested format.
        """
        if len(self._numeric_parts) < num_parts:
            raise ValueError(f"Version '{self}' does not have enough components for requested format.")
        truncated_version_str = ".".join(map(str, self._numeric_parts[:num_parts]))
        return Version(truncated_version_str)
