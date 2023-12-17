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
