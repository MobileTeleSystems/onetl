from enum import Enum


class DBWriteMode(Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"
    ERROR = "error"
