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

from typing import Optional, Union

from onetl.base.path_stat_protocol import PathStatProtocol
from onetl.impl.frozen_model import FrozenModel


class RemotePathStat(FrozenModel):
    st_size: int = 0
    """
    Size of file, in bytes
    """

    st_mtime: Optional[float] = None
    """
    Unix timestamp of most recent content modification
    """

    st_mode: Optional[int] = None
    """
    File mode bits
    """

    st_uid: Union[int, str, None] = None
    """
    User identifier of the file owner
    """

    st_gid: Union[int, str, None] = None
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
