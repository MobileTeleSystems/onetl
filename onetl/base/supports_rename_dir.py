# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import os
from typing import Protocol, runtime_checkable

from onetl.base.path_protocol import PathWithStatsProtocol


@runtime_checkable
class SupportsRenameDir(Protocol):
    """
    Protocol for objects containing `rename_dir` method.

    !!! success "Added in 0.8.0"
    """

    def rename_dir(
        self,
        source_dir_path: str | os.PathLike,
        target_dir_path: str | os.PathLike,
        *,
        replace: bool = False,
    ) -> PathWithStatsProtocol: ...
