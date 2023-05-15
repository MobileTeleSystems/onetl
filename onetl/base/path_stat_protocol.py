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
