# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import os
import sys
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    LocalPath: TypeAlias = Path
else:

    class LocalPath(Path):
        def __new__(cls, *args, **kwargs):
            if cls is LocalPath:
                cls = LocalWindowsPath if os.name == "nt" else LocalPosixPath  # noqa: PLW0642
            if sys.version_info < (3, 12):
                return cls._from_parts(args)
            return object.__new__(cls)

    class LocalPosixPath(LocalPath, PurePosixPath):
        pass

    class LocalWindowsPath(LocalPath, PureWindowsPath):
        pass
