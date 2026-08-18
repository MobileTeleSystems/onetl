# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from onetl.base.path_stat_protocol import PathStatProtocol
from onetl.impl.frozen_model import FrozenModel


class RemotePathStat(FrozenModel):
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

    st_uid: int | str | None = None
    """
    User identifier of the file owner
    """

    st_gid: int | str | None = None
    """
    Group identifier of the file owner
    """

    @classmethod
    def from_stat(cls, path_stat: PathStatProtocol) -> "RemotePathStat":
        return cls(
            st_size=path_stat.st_size,
            st_mtime=path_stat.st_mtime,
            st_mode=path_stat.st_mode,
            st_uid=path_stat.st_uid,
            st_gid=path_stat.st_gid,
        )
