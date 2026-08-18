# SPDX-FileCopyrightText: 2025-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pydantic import ByteSize, model_validator

from onetl.base import BaseFileFilter, PathProtocol
from onetl.base.path_protocol import PathWithStatsProtocol
from onetl.impl import FrozenModel


class FileSizeRange(BaseFileFilter, FrozenModel):
    """Filter files matching a specified size.

    If file size (`.stat().st_size`) doesn't match the range, it will be excluded.
    Doesn't affect directories or paths without `.stat()` method.

    !!! success "Added in 0.13.0"

    !!! note

        [SI unit prefixes](https://en.wikipedia.org/wiki/Byte#Multiple-byte_units)
        means that `1KB` == `1 kilobyte` == `1000 bytes`.
        If you need `1024 bytes`, use `1 KiB` == `1 kibibyte`.

    Parameters
    ----------

    min

        Minimal allowed file size. `None` means no limit.

    max

        Maximum allowed file size. `None` means no limit.

    Examples
    --------

    Specify min and max file sizes:

    ```python
    from onetl.file.filter import FileSizeRange

    file_size = FileSizeRange(min="1KiB", max="100MiB")

    ```
    Specify only min file size:

    ```python
    from onetl.file.filter import FileSizeRange

    file_size = FileSizeRange(min="1KiB")

    ```
    Specify only max file size:

    ```python
    from onetl.file.filter import FileSizeRange

    file_size = FileSizeRange(max="100MiB")

    ```
    """

    min: ByteSize | None = None
    max: ByteSize | None = None

    @model_validator(mode="after")
    def _validate_min_max(self):
        if self.min is None and self.max is None:
            msg = "Either min or max must be specified"
            raise ValueError(msg)

        if self.min is not None and self.max is not None and self.min > self.max:
            msg = "Min size cannot be greater than max size"
            raise ValueError(msg)

        return self

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
