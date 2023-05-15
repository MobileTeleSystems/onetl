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
from dataclasses import dataclass, field

from onetl.base import PathStatProtocol
from onetl.impl.path_container import PathContainer
from onetl.impl.remote_path import RemotePath
from onetl.impl.remote_path_stat import RemotePathStat


@dataclass(eq=False, frozen=True)
class RemoteDirectory(PathContainer[RemotePath]):
    """
    Representation of existing remote directory
    """

    stats: PathStatProtocol = field(default_factory=RemotePathStat)

    def __post_init__(self):
        # frozen=True does not allow to change any field in __post_init__, small hack here
        object.__setattr__(self, "path", RemotePath(self.path))  # noqa: WPS609

    def is_dir(self) -> bool:
        return True

    def is_file(self) -> bool:
        return False

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

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({os.fspath(self.path)!r})"
