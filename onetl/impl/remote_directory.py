# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from onetl.base import PathProtocol, PathStatProtocol
from onetl.impl.path_container import PathContainer
from onetl.impl.remote_path import RemotePath
from onetl.impl.remote_path_stat import RemotePathStat

if TYPE_CHECKING:

    class RemoteDirectory(PathProtocol, RemotePath):
        def __init__(self, path: RemotePath, stats: PathStatProtocol): ...

        @property
        def path(self) -> RemotePath: ...

        @property
        def stats(self) -> PathStatProtocol: ...

else:

    @dataclass(eq=False, frozen=True)
    class RemoteDirectory(PathContainer[RemotePath]):
        """
        Representation of existing remote directory
        """

        stats: PathStatProtocol = field(default_factory=RemotePathStat)

        def __post_init__(self):
            # frozen=True does not allow to change any field in __post_init__, small hack here
            object.__setattr__(self, "path", RemotePath(self.path))

        def is_dir(self) -> bool:
            return True

        def is_file(self) -> bool:
            return False

        def exists(self) -> bool:
            return True

        def stat(self) -> PathStatProtocol:
            return self.stats

        @property
        def parent(self) -> "RemoteDirectory":
            return RemoteDirectory(self.path.parent)

        @property
        def parents(self) -> "list[RemoteDirectory]":
            return [RemoteDirectory(parent) for parent in self.path.parents]

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({os.fspath(self.path)!r})"
