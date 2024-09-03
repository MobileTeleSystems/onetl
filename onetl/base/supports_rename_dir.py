# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os

from typing_extensions import Protocol, runtime_checkable

from onetl.base.path_protocol import PathWithStatsProtocol


@runtime_checkable
class SupportsRenameDir(Protocol):
    """
    Protocol for objects containing ``rename_dir`` method.

    .. versionadded:: 0.8.0
    """

    def rename_dir(
        self,
        source_dir_path: str | os.PathLike,
        target_dir_path: str | os.PathLike,
        replace: bool = False,
    ) -> PathWithStatsProtocol: ...
