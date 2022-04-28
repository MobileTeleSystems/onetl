from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath

from onetl.base import FileStatProtocol
from onetl.impl.path_container import PathContainer


@dataclass(eq=False, frozen=True)
class RemoteFile(PathContainer[PurePosixPath]):
    """
    Representation of existing remote file with stat
    """

    stats: FileStatProtocol

    def __post_init__(self):
        # frozen=True does not allow to change any field in __post_init__, small hack here
        object.__setattr__(self, "path", PurePosixPath(self.path))  # noqa: WPS609

    def is_dir(self) -> bool:
        return False

    def is_file(self) -> bool:
        return True

    def exists(self) -> bool:
        return True

    def stat(self) -> FileStatProtocol:
        return self.stats


@dataclass(eq=False, frozen=True)
class FailedRemoteFile(RemoteFile):
    """
    Representation of existing remote file with stat and attached exception object
    """

    exception: Exception

    # exceptions are not allowed to compare, small hack here
    def _compare_tuple(self, args) -> tuple:
        return tuple(str(arg) if isinstance(arg, Exception) else arg for arg in args)
