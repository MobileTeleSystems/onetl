# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import re
import stat
import textwrap
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePath

from humanize import naturalsize

from onetl.base import ContainsException, PathProtocol, PathWithStatsProtocol

FILE_TYPE_DETECTORS = {
    stat.S_ISSOCK: "socket",
    stat.S_ISLNK: "link",
    stat.S_ISREG: "file",
    stat.S_ISBLK: "block",
    stat.S_ISDIR: "directory",
    stat.S_ISCHR: "char",
    stat.S_ISFIFO: "fifo",
}


@dataclass
class PathRepr:
    path: str
    kind: str | None = None
    mode: int | None = None
    size: int | None = None
    mtime: datetime | None = None
    user: str | int | None = None
    group: str | int | None = None
    exception: Exception | None = None

    @classmethod
    def from_path(cls, path) -> PathRepr:
        if isinstance(path, str):
            path = PurePath(path)

        result = cls(path=os.fspath(path))

        if isinstance(path, PathProtocol):
            result.kind = cls.detect_kind(path)

            if path.exists() and isinstance(path, PathWithStatsProtocol):
                details = path.stat()
                result.size = details.st_size
                result.mtime = datetime.fromtimestamp(details.st_mtime, tz=timezone.utc) if details.st_mtime else None
                result.mode = details.st_mode
                result.user = details.st_uid
                result.group = details.st_gid

        if isinstance(path, ContainsException):
            result.exception = path.exception

        return result

    @staticmethod
    def detect_kind(path: PathProtocol) -> str:
        if not path.exists():
            return "missing"

        if isinstance(path, PathWithStatsProtocol):
            mode = path.stat().st_mode
            if mode is not None:
                # try to detect mode based on stats
                for detector, file_kind in FILE_TYPE_DETECTORS.items():
                    if detector(mode):
                        return file_kind  # noqa: WPS220

        if path.is_dir():
            return "directory"

        if path.is_file():
            return "file"

        return "unknown"

    def repr_size(self) -> str:
        if self.size is not None and self.kind == "file":
            return f"size={naturalsize(self.size)!r}"

        return ""

    def repr_kind(self) -> str:
        if not self.kind:
            return ""

        return f"kind={self.kind!r}"

    def repr_mtime(self) -> str:
        if not self.mtime:
            return ""

        return f"mtime={self.mtime.isoformat()!r}"

    def repr_mode(self) -> str:
        if self.mode is None:
            return ""

        # first symbol is a file type, which is already stored in `kind` attribute
        return f"mode={stat.filemode(self.mode)[1:]!r}"

    def repr_user(self) -> str:
        if not self.user:
            return ""

        return f"uid={self.user!r}"

    def repr_group(self) -> str:
        if not self.group:
            return ""

        return f"gid={self.group!r}"

    def repr_exception(self) -> str:
        if not self.exception:
            return ""

        prefix = " " * 4
        exception = re.sub(r"(\\r)?\\n", os.linesep, repr(self.exception))
        exception_formatted = textwrap.indent(exception, prefix)
        return os.linesep + exception_formatted + os.linesep

    def info(
        self,
        with_kind: bool = True,
        with_size: bool = True,
        with_mode: bool = True,
        with_mtime: bool = True,
        with_owner: bool = True,
        with_exception: bool = True,
    ):
        result = repr(self.path)
        properties: list[str] = []
        if with_kind:
            properties.append(self.repr_kind())

        if with_size:
            properties.append(self.repr_size())

        if with_mode:
            properties.append(self.repr_mode())

        if with_mtime:
            properties.append(self.repr_mtime())

        if with_owner:
            properties.append(self.repr_user())
            properties.append(self.repr_group())

        properties = list(filter(None, properties))

        result_str = f"{result} ({', '.join(properties)})" if properties else result

        if with_exception:
            result_str += self.repr_exception()

        return result_str


def path_repr(
    path: os.PathLike | str,
    with_kind: bool = True,
    with_size: bool = True,
    with_mode: bool = True,
    with_mtime: bool = True,
    with_owner: bool = True,
    with_exception: bool = True,
) -> str:
    return PathRepr.from_path(path).info(
        with_kind=with_kind,
        with_size=with_size,
        with_mode=with_mode,
        with_mtime=with_mtime,
        with_owner=with_owner,
        with_exception=with_exception,
    )
