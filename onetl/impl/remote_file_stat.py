from __future__ import annotations

from dataclasses import dataclass

from onetl.base.file_stat_protocol import FileStatProtocol


@dataclass(frozen=True)
class RemoteFileStat:
    st_size: int
    st_mtime: float

    @classmethod
    def from_stat(cls, stat: FileStatProtocol) -> RemoteFileStat:
        return cls(st_size=stat.st_size, st_mtime=stat.st_mtime)
