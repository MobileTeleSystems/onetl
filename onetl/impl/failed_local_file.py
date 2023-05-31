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

from onetl.impl.local_path import LocalPath
from onetl.impl.path_container import PathContainer


@dataclass(eq=False, frozen=True)
class FailedLocalFile(PathContainer[LocalPath]):
    """
    Representation of existing local file with stat and attached exception object
    """

    exception: Exception

    def __post_init__(self):
        # frozen=True does not allow to change any field in __post_init__, small hack here
        object.__setattr__(self, "path", LocalPath(self.path))  # noqa: WPS609

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
