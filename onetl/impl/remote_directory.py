from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath

from onetl.impl.path_container import PathContainer


@dataclass(eq=False, frozen=True)
class RemoteDirectory(PathContainer[PurePosixPath]):
    """
    Representation of existing remote directory
    """

    def __post_init__(self):
        # frozen=True does not allow to change any field in __post_init__, small hack here
        object.__setattr__(self, "path", PurePosixPath(self.path))  # noqa: WPS609

    def is_dir(self) -> bool:
        return True

    def is_file(self) -> bool:
        return False

    def exists(self) -> bool:
        return True
