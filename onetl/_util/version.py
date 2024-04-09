# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import re
from functools import total_ordering


@total_ordering
class Version:
    """
    Version representation.

    """

    def __init__(self, version):
        if version is None:
            self._raw_str = ""
            self._raw_parts = []
            self._numeric_parts = []
        elif isinstance(version, Version):
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
        return self._numeric_parts[0] if self._numeric_parts else 0

    @property
    def minor(self) -> int:
        return self._numeric_parts[1] if len(self._numeric_parts) > 1 else 0

    @property
    def patch(self) -> int:
        return self._numeric_parts[2] if len(self._numeric_parts) > 2 else 0

    @property
    def raw_parts(self) -> list:
        return self._raw_parts

    def __getitem__(self, item):
        return self._numeric_parts[item]

    def __len__(self):
        return len(self._numeric_parts)

    def __str__(self):
        return ".".join(self._raw_parts)

    def __eq__(self, other):
        return self._numeric_parts == other._numeric_parts

    def __lt__(self, other):
        return self._numeric_parts < other._numeric_parts

    def __gt__(self, other):
        return self._numeric_parts > other._numeric_parts

    def __ge__(self, other):
        return self._numeric_parts >= other._numeric_parts

    def digits(self, num_parts: int) -> Version:
        """Returns a version object truncated to the first version components."""
        if len(self._numeric_parts) < num_parts:
            raise ValueError(f"Version '{self}' does not have enough components for requested format.")
        truncated_version_str = ".".join(map(str, self._numeric_parts[:num_parts]))
        return Version(truncated_version_str)
