# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing_extensions import Protocol, runtime_checkable


@runtime_checkable
class PathStatProtocol(Protocol):
    """
    Protocol for ``os.stat_result``-like objects.

    Includes only minimal set of fields supported by any file system
    """

    @property
    def st_size(self) -> int:
        """
        Size of file, in bytes
        """

    @property
    def st_mtime(self) -> float | None:
        """
        Unix timestamp of most recent content modification
        """

    @property
    def st_uid(self) -> str | int | None:
        """
        User identifier of the file owner
        """

    @property
    def st_gid(self) -> str | int | None:
        """
        Group identifier of the file owner
        """

    @property
    def st_mode(self) -> int | None:
        """
        File mode bits
        """
