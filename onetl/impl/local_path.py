# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import os
import sys
from pathlib import Path, PurePosixPath, PureWindowsPath


class LocalPath(Path):
    def __new__(cls, *args, **kwargs):
        if cls is LocalPath:
            cls = LocalWindowsPath if os.name == "nt" else LocalPosixPath
        if sys.version_info < (3, 12):
            return cls._from_parts(args)
        else:
            return object.__new__(cls)  # noqa: WPS503


class LocalPosixPath(LocalPath, PurePosixPath):
    pass


class LocalWindowsPath(LocalPath, PureWindowsPath):
    pass
