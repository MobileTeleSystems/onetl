class DirectoryNotFoundError(OSError):
    """
    Like ``FileNotFoundError``, but for directory.

    Cannot be replaced with ``NotAFileError`` because on some operating systems
    (e.g. Linux) there are other file types than regular file and directory - symlink, device, etc
    """


class NotAFileError(OSError):
    """
    Like ``NotADirectoryError``, but for files.

    Cannot be replaced with ``FileNotFoundError``, it has different meaning
    """


class DirectoryNotEmptyError(OSError):
    """
    Is raised when trying to remove directory contains some files or other directories
    """
