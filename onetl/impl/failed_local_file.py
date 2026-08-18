# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

from onetl.impl.local_path import LocalPath
from onetl.impl.path_container import PathContainer

if TYPE_CHECKING:

    class FailedLocalFile(LocalPath):
        def __init__(self, path: LocalPath, exception: Exception): ...

        @property
        def exception(self) -> Exception: ...
else:

    @dataclass(eq=False, frozen=True, slots=True)
    class FailedLocalFile(PathContainer[LocalPath]):
        """
        Representation of existing local file with stat and attached exception object
        """

        exception: Exception

        def __post_init__(self):
            # frozen=True does not allow to change any field in __post_init__, small hack here
            object.__setattr__(self, "path", LocalPath(self.path))

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}({os.fspath(self.path)!r}, {self.exception!r})"

        def exists(self) -> bool:
            return self.path.exists()

        def is_file(self) -> bool:
            return self.path.is_file()

        def is_dir(self) -> bool:
            return self.path.is_dir()

        def stat(self) -> os.stat_result:
            return self.path.stat()

        # exceptions are not allowed to compare, another small hack
        def _compare_tuple(self, args) -> tuple:
            return tuple(str(arg) if isinstance(arg, Exception) else arg for arg in args)
