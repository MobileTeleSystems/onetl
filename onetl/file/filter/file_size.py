# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import Optional

from onetl.base.path_protocol import PathWithStatsProtocol

try:
    from pydantic.v1 import ByteSize, root_validator, validator
except (ImportError, AttributeError):
    from pydantic import ByteSize, root_validator, validator  # type: ignore[no-redef, assignment]

from onetl.base import BaseFileFilter, PathProtocol
from onetl.impl import FrozenModel


class FileSizeRange(BaseFileFilter, FrozenModel):
    """Filter files matching a specified size.

    If file size (``.stat().st_size``) doesn't match the range, it will be excluded.
    Doesn't affect directories or paths without ``.stat()`` method.

    .. versionadded:: 0.13.0

    .. note::

        `SI unit prefixes <https://en.wikipedia.org/wiki/Byte#Multiple-byte_units>`_
        means that ``1KB`` == ``1 kilobyte`` == ``1000 bytes``.
        If you need ``1024 bytes``, use ``1 KiB`` == ``1 kibibyte``.

    Parameters
    ----------

    min : int or str, optional

        Minimal allowed file size. ``None`` means no limit.

    max : int or str, optional

        Maximum allowed file size. ``None`` means no limit.

    Examples
    --------

    Specify min and max file sizes:

    .. code:: python

        from onetl.file.filter import FileSizeRange

        file_size = FileSizeRange(min="1KiB", max="100MiB")

    Specify only min file size:

    .. code:: python

        from onetl.file.filter import FileSizeRange

        file_size = FileSizeRange(min="1KiB")

    Specify only max file size:

    .. code:: python

        from onetl.file.filter import FileSizeRange

        file_size = FileSizeRange(max="100MiB")
    """

    min: Optional[ByteSize] = None
    max: Optional[ByteSize] = None

    @root_validator(skip_on_failure=True)
    def _validate_min_max(cls, values):
        min_value = values.get("min")
        max_value = values.get("max")

        if min_value is None and max_value is None:
            raise ValueError("Either min or max must be specified")

        if min_value and max_value and min_value > max_value:
            raise ValueError("Min size cannot be greater than max size")

        return values

    @validator("min", "max")
    def _validate_min(cls, value):
        if value is not None and value < 0:
            raise ValueError("size cannot be negative")
        return value

    def __repr__(self):
        min_human_readable = self.min.human_readable() if self.min is not None else None
        max_human_readable = self.max.human_readable() if self.max is not None else None
        return f"{self.__class__.__name__}(min={min_human_readable!r}, max={max_human_readable!r})"

    def match(self, path: PathProtocol) -> bool:
        if path.is_file() and isinstance(path, PathWithStatsProtocol):
            file_size = path.stat().st_size

            if self.min is not None and file_size < self.min:
                return False

            if self.max is not None and file_size > self.max:
                return False

        return True
