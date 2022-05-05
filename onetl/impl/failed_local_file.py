from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from onetl.impl.path_container import PathContainer


@dataclass(eq=False, frozen=True)
class FailedLocalFile(PathContainer[Path]):
    """
    Representation of existing local file with stat and attached exception object
    """

    exception: Exception

    def __post_init__(self):
        # frozen=True does not allow to change any field in __post_init__, small hack here
        object.__setattr__(self, "path", Path(self.path))  # noqa: WPS609

    # exceptions are not allowed to compare, another small hack
    def _compare_tuple(self, args) -> tuple:
        return tuple(str(arg) if isinstance(arg, Exception) else arg for arg in args)
