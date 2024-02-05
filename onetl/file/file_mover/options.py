# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings

from pydantic import Field, root_validator

from onetl.impl import FileExistBehavior, GenericOptions


class FileMoverOptions(GenericOptions):
    """File moving options"""

    if_exists: FileExistBehavior = Field(default=FileExistBehavior.ERROR, alias="mode")
    """
    How to handle existing files in the local directory.

    Possible values:
        * ``error`` (default) - do nothing, mark file as failed
        * ``ignore`` - do nothing, mark file as ignored
        * ``replace_file`` - replace existing file with a new one
        * ``replace_entire_directory`` - delete directory content before moving files
    """

    workers: int = Field(default=1, ge=1)
    """
    Number of workers to create for parallel file moving.

    1 (default) means files will me moved sequentially.
    2 or more means files will be moved in parallel workers.

    Recommended value is ``min(32, os.cpu_count() + 4)``, e.g. ``5``.
    """

    @root_validator(pre=True)
    def _mode_is_deprecated(cls, values):
        if "mode" in values:
            warnings.warn(
                "Option `FileMover.Options(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `FileMover.Options(if_exists=...)` instead",
                category=UserWarning,
                stacklevel=3,
            )
        return values
