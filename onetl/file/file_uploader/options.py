# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings

try:
    from pydantic.v1 import Field, root_validator
except (ImportError, AttributeError):
    from pydantic import Field, root_validator  # type: ignore[no-redef, assignment]

from onetl.impl import FileExistBehavior, GenericOptions


class FileUploaderOptions(GenericOptions):
    """File uploading options.

    .. versionadded:: 0.3.0
    """

    if_exists: FileExistBehavior = Field(default=FileExistBehavior.ERROR, alias="mode")
    """
    How to handle existing files in the target directory.

    Possible values:
        * ``error`` (default) - mark file as failed
        * ``ignore`` - mark file as skipped
        * ``replace_file`` - replace existing file with a new one
        * ``replace_entire_directory`` - delete local directory content before downloading files

    .. versionchanged:: 0.9.0
        Renamed ``mode`` → ``if_exists``
    """

    delete_local: bool = False
    """
    If ``True``, remove local file after successful download.

    If download failed, file will left intact.

    .. versionadded:: 0.2.0

    .. versionchanged:: 0.3.0
        Move ``FileUploader.delete_local`` to ``FileUploaderOptions``
    """

    workers: int = Field(default=1, ge=1)
    """
    Number of workers to create for parallel file upload.

    1 (default) means files will me uploaded sequentially.
    2 or more means files will be uploaded in parallel workers.

    Recommended value is ``min(32, os.cpu_count() + 4)``, e.g. ``5``.

    .. versionadded:: 0.8.1
    """

    @root_validator(pre=True)
    def _mode_is_deprecated(cls, values):
        if "mode" in values:
            warnings.warn(
                "Option `FileUploader.Options(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `FileUploader.Options(if_exists=...)` instead",
                category=UserWarning,
                stacklevel=3,
            )
        return values
