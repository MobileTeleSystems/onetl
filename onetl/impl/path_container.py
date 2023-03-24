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
from dataclasses import astuple, dataclass
from typing import Generic, TypeVar

GenericPath = TypeVar("GenericPath", bound=os.PathLike)


@dataclass(eq=False, frozen=True)
class PathContainer(Generic[GenericPath]):
    """
    Read-only container for ``pathlib.PurePath``-like classes with some custom logic.

    This is need to avoid creating objects of current class using ``PurePath``-like methods
    (e.g. ``path.with_name("new_name")``) or operators (e.g. ``path / "sub_path"``).

    It proxies all the method calls and operators to the underlying ``path`` field,
    so every method or operator returns new object of the same class as ``path`` field, without any magic.
    """

    path: GenericPath

    def __str__(self) -> str:
        return str(self.path)

    def __bytes__(self):
        return os.fsencode(self.path)

    def __fspath__(self) -> str:
        return os.fspath(self.path)

    def __getattr__(self, attr: str):
        return getattr(self.path, attr)

    def __truediv__(self, other) -> GenericPath:
        return self.path / other

    def __rtruediv__(self, other) -> GenericPath:
        return other / self.path

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        if not isinstance(other, PathContainer):
            return self.path == other

        return astuple(self, tuple_factory=self._compare_tuple) == astuple(other, tuple_factory=self._compare_tuple)

    def __lt__(self, other):
        if not isinstance(other, PathContainer):
            return self.path < other

        return self.path < other.path

    def __le__(self, other):
        if not isinstance(other, PathContainer):
            return self.path <= other

        return self.path <= other.path

    def __gt__(self, other):
        if not isinstance(other, PathContainer):
            return self.path > other

        return self.path > other.path

    def __ge__(self, other):
        if not isinstance(other, PathContainer):
            return self.path >= other

        return self.path >= other.path

    def _compare_tuple(self, args) -> tuple:
        return tuple(args)
