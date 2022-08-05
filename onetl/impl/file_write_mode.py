from enum import Enum


class FileWriteMode(str, Enum):  # noqa: WPS600
    ERROR = "error"
    IGNORE = "ignore"
    OVERWRITE = "overwrite"
    DELETE_ALL = "delete_all"

    def __str__(self):
        return str(self.value)
