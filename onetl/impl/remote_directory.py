from __future__ import annotations

import os
from dataclasses import dataclass

from onetl.impl.path_container import PathContainer
from onetl.impl.remote_path import RemotePath


@dataclass(eq=False, frozen=True)
class RemoteDirectory(PathContainer[RemotePath]):
    """
    Representation of existing remote directory
    """

    def __post_init__(self):
        # frozen=True does not allow to change any field in __post_init__, small hack here
        object.__setattr__(self, "path", RemotePath(self.path))  # noqa: WPS609

    def is_dir(self) -> bool:
        return True

    def is_file(self) -> bool:
        return False

    def exists(self) -> bool:
        return True

    @property
    def parent(self) -> RemoteDirectory:
        return RemoteDirectory(self.path.parent)

    @property
    def parents(self) -> list[RemoteDirectory]:
        return [RemoteDirectory(parent) for parent in self.path.parents]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{os.fspath(self.path)}')"
