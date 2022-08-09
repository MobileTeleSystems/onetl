from __future__ import annotations

import os
import re
import textwrap
from pathlib import PurePath

from humanize import naturalsize

from onetl.base import ContainsException, PathProtocol, SizedPathProtocol


def humanize_path(path: os.PathLike | str) -> str:
    if isinstance(path, (str, bytes)):
        path = PurePath(path)

    result = repr(os.fspath(path))

    if isinstance(path, PathProtocol):
        if not path.exists():
            result = f"{result} (missing)"

        elif path.is_dir():
            result = f"{result} (directory)"

        elif isinstance(path, SizedPathProtocol):
            size = naturalsize(path.stat().st_size)
            result = f"{result} ({size})"

        elif path.is_file():
            result = f"{result} (file)"

    if not isinstance(path, ContainsException):
        return result

    prefix = " " * 4
    exception = re.sub(r"(\\r)?\\n", os.linesep, repr(path.exception))
    exception_formatted = textwrap.indent(exception, prefix)

    return os.linesep.join([result, exception_formatted, ""])
