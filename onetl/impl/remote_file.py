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

import os
from dataclasses import dataclass

from onetl.base import PathStatProtocol
from onetl.impl.path_container import PathContainer
from onetl.impl.remote_directory import RemoteDirectory
from onetl.impl.remote_path import RemotePath


@dataclass(eq=False, frozen=True)
class RemoteFile(PathContainer[RemotePath]):
    """
    Representation of existing remote file with stat
    """

    stats: PathStatProtocol

    def __post_init__(self):
        # frozen=True does not allow to change any field in __post_init__, small hack here
        object.__setattr__(self, "path", RemotePath(self.path))  # noqa: WPS609

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({os.fspath(self.path)!r})"

    def is_dir(self) -> bool:
        return False

    def is_file(self) -> bool:
        return True

    def exists(self) -> bool:
        return True

    def stat(self) -> PathStatProtocol:
        return self.stats

    @property
    def parent(self) -> RemoteDirectory:
        return RemoteDirectory(self.path.parent)

    @property
    def parents(self) -> list[RemoteDirectory]:
        return [RemoteDirectory(parent) for parent in self.path.parents]


@dataclass(eq=False, frozen=True)
class FailedRemoteFile(RemoteFile):
    """
    Representation of existing remote file with stat and attached exception object
    """

    exception: Exception

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({os.fspath(self.path)!r}, {self.exception!r})"

    # exceptions are not allowed to compare, small hack here
    def _compare_tuple(self, args) -> tuple:
        return tuple(str(arg) if isinstance(arg, Exception) else arg for arg in args)
