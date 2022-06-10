import os
from pathlib import Path, PurePosixPath, PureWindowsPath


class LocalPath(Path):
    def __new__(cls, *args, **kwargs):
        if cls is LocalPath:
            cls = LocalWindowsPath if os.name == "nt" else LocalPosixPath
        self = cls._from_parts(args, init=False)
        self._init()
        return self


class LocalPosixPath(LocalPath, PurePosixPath):
    pass  # noqa: WPS604, WPS420


class LocalWindowsPath(LocalPath, PureWindowsPath):
    pass  # noqa: WPS604, WPS420
