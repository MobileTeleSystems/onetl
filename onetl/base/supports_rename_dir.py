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

from typing_extensions import Protocol, runtime_checkable

from onetl.base.path_protocol import PathWithStatsProtocol


@runtime_checkable
class SupportsRenameDir(Protocol):
    """
    Protocol for objects containing ``rename_dir`` method
    """

    def rename_dir(
        self,
        source_dir_path: str | os.PathLike,
        target_dir_path: str | os.PathLike,
        replace: bool = False,
    ) -> PathWithStatsProtocol:
        ...
