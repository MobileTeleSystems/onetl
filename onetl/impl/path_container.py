# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
from dataclasses import astuple, dataclass
from typing import Generic, Sequence, TypeVar

from onetl.base import PurePathProtocol

PurePath = TypeVar("PurePath", bound=PurePathProtocol)


@dataclass(eq=False, frozen=True)
class PathContainer(Generic[PurePath]):
    """
    Read-only container for ``pathlib.PurePath``-like classes with some custom logic.

    This is need to avoid creating objects of current class using ``PurePath``-like methods
    (e.g. ``path.with_name("new_name")``) or operators (e.g. ``path / "sub_path"``).

    It proxies all the method calls and operators to the underlying ``path`` field,
    so every method or operator returns new object of the same class as ``path`` field, without any magic.
    """

    path: PurePath

    def __str__(self) -> str:
        return str(self.path)

    def __bytes__(self):
        return os.fsencode(self.path)

    def __fspath__(self) -> str:
        return os.fspath(self.path)

    def __truediv__(self, other) -> PurePath:
        return self.path / other

    def __rtruediv__(self, other) -> PurePath:
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

    # typing_extensions 4.6+ implementation of `isinstance`` does not use __getattr__ anymore,
    # so we need to create methods explicitly
    @property
    def name(self) -> str:
        return self.path.name

    @property
    def parent(self) -> PurePathProtocol | PurePath:
        return self.path.parent

    @property
    def parents(self) -> Sequence[PurePathProtocol | PurePath]:
        return self.path.parents

    @property
    def parts(self) -> Sequence[str]:
        return self.path.parts

    def is_absolute(self) -> bool:
        return self.path.is_absolute()

    def match(self, path_pattern) -> bool:
        return self.path.match(path_pattern)

    def relative_to(self, *other) -> PurePathProtocol | PurePath:
        return self.path.relative_to(*other)

    def as_posix(self) -> str:
        return self.path.as_posix()

    def joinpath(self, *args) -> PurePathProtocol | PurePath:
        return self.path.joinpath(*args)

    def _compare_tuple(self, args) -> tuple:
        return tuple(args)
