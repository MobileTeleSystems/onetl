# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
import warnings
from enum import Enum

log = logging.getLogger(__name__)


class FileExistBehavior(str, Enum):
    ERROR = "error"
    IGNORE = "ignore"
    REPLACE_FILE = "replace_file"
    REPLACE_ENTIRE_DIRECTORY = "replace_entire_directory"

    def __str__(self):
        return str(self.value)

    @classmethod  # noqa: WPS120
    def _missing_(cls, value: object):  # noqa: WPS120
        if str(value) == "overwrite":
            warnings.warn(
                "Mode `overwrite` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `replace_file` instead",
                category=UserWarning,
                stacklevel=4,
            )
            return cls.REPLACE_FILE

        if str(value) == "delete_all":
            warnings.warn(
                "Mode `delete_all` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `replace_entire_directory` instead",
                category=UserWarning,
                stacklevel=4,
            )
            return cls.REPLACE_ENTIRE_DIRECTORY
