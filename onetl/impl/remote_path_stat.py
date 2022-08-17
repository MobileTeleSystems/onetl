from __future__ import annotations

from dataclasses import dataclass

from onetl.base.path_stat_protocol import PathStatProtocol


@dataclass(frozen=True)
class RemotePathStat:
    st_size: int = 0
    """
    Size of file, in bytes
    """

    st_mtime: float | None = None
    """
    Unix timestamp of most recent content modification
    """

    st_mode: int | None = None
    """
    File mode bits
    """

    st_uid: str | int | None = None
    """
    User identifier of the file owner
    """

    st_gid: str | int | None = None
    """
    Group identifier of the file owner
    """

    @classmethod
    def from_stat(cls, path_stat: PathStatProtocol) -> RemotePathStat:
        return cls(
            st_size=path_stat.st_size,
            st_mtime=path_stat.st_mtime,
            st_mode=path_stat.st_mode,
            st_uid=path_stat.st_uid,
            st_gid=path_stat.st_gid,
        )
