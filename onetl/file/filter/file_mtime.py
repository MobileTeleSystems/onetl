# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import datetime
from typing import Optional

from onetl.base.path_protocol import PathWithStatsProtocol

try:
    from pydantic.v1 import root_validator, validator
except (ImportError, AttributeError):
    from pydantic import root_validator, validator  # type: ignore[no-redef, assignment]

from onetl.base import BaseFileFilter, PathProtocol
from onetl.impl import FrozenModel


class FileModifiedTime(BaseFileFilter, FrozenModel):
    """Filter files matching a specified modification time.

    If file modification time (``.stat().st_mtime``) doesn't match range, it will be excluded.
    Doesn't affect directories or paths without ``.stat()`` method.

    .. note::

        Some filesystems return timestamps truncated to whole seconds (without millisecond part).
        obj:`~since` and :obj`~until`` values should be adjusted accordingly.

    .. versionadded:: 0.13.0

    Parameters
    ----------

    since : datetime, optional

        Minimal allowed file modification time. ``None`` means no limit.

    until : datetime, optional

        Maximum allowed file modification time. ``None`` means no limit.

    Examples
    --------

    Select files modified between start of the day (``00:00:00``) and hour ago:

    .. code:: python

        from datetime import datetime, timedelta
        from onetl.file.filter import FileModifiedTime

        hour_ago = datetime.now() - timedelta(hours=1)
        day_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        file_mtime = FileModifiedTime(since=day_start, until=hour_ago)

    Select only files modified since hour ago:

    .. code:: python

        from datetime import datetime, timedelta
        from onetl.file.filter import FileModifiedTime

        hour_ago = datetime.now() - timedelta(hours=1)
        file_mtime = FileModifiedTime(since=hour_ago)
    """

    since: Optional[datetime] = None
    until: Optional[datetime] = None

    @root_validator(skip_on_failure=True)
    def _validate_since_until(cls, values):
        since = values.get("since")
        until = values.get("until")

        if since is None and until is None:
            raise ValueError("Either since or until must be specified")

        # since and until can be tz-naive and tz-aware, which are cannot be compared.
        if since and until and since.timestamp() > until.timestamp():
            raise ValueError("since cannot be greater than until")

        return values

    @validator("since", "until", pre=True)
    def _parse_isoformat(cls, value):
        if isinstance(value, str):
            # Pydantic doesn't allow values like "YYYY-MM-DD" as input, but .fromisoformat() does
            return datetime.fromisoformat(value)
        return value

    @validator("since", "until")
    def _always_include_tz(cls, value):
        if value.tzinfo is None:
            # tz-naive datetime should be converted to tz-aware
            return value.astimezone()
        return value

    def __repr__(self):
        since_human_readable = self.since.isoformat() if self.since is not None else None
        until_human_readable = self.until.isoformat() if self.until is not None else None
        return f"{self.__class__.__name__}(since={since_human_readable!r}, until={until_human_readable!r})"

    def match(self, path: PathProtocol) -> bool:
        if path.is_file() and isinstance(path, PathWithStatsProtocol):
            file_mtime = path.stat().st_mtime
            if not file_mtime:
                return True

            # mtime is always POSIX timestamp, avoid converting it to datetime and dancing with timezones
            if self.since is not None and file_mtime < self.since.timestamp():
                return False

            if self.until is not None and file_mtime > self.until.timestamp():
                return False

        return True
