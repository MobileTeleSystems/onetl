from enum import Enum


class FileWriteMode(Enum):
    ERROR = "error"
    IGNORE = "ignore"
    OVERWRITE = "overwrite"
    DELETE_ALL = "delete_all"
