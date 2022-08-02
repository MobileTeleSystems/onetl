from evacuator import NeedEvacuation


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
    Raised when trying to remove directory contains some files or other directories
    """


class NoDataError(NeedEvacuation):
    """
    Raised when there is no data in FileResult or DataFrame
    """


class FilesError(RuntimeError):
    """
    Raised when something went wrong while working with file collection
    """


class SkippedFilesError(FilesError):
    """
    Raised when file collection contains skipped files
    """


class FailedFilesError(FilesError):
    """
    Raised when file collection contains failed files
    """


class MissingFilesError(FilesError):
    """
    Raised when file collection contains missing files
    """


class ZeroFileSizeError(FilesError):
    """
    Raised when file collection contains some zero-sized file
    """


class EmptyFilesError(FilesError, NoDataError):
    """
    Raised when file collection is empty
    """
