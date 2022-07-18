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


class FileResultError(RuntimeError):
    """
    Raised when something went wrong while working with FileResult object
    """


class SkippedFileResultError(FileResultError):
    """
    Raised when FileResult object contains skipped files
    """


class FailedFileResultError(FileResultError):
    """
    Raised when FileResult object contains failed files
    """


class MissingFileResultError(FileResultError):
    """
    Raised when FileResult object contains missing files
    """


class ZeroSizeFileResultError(FileResultError):
    """
    Raised when FileResult object contains any zero-sized file
    """


class NoSuccessfulFileResultError(FileResultError):
    """
    Raised when FileResult object does not contain any successful files
    """


class EmptyFileResultError(FileResultError, NoDataError):
    """
    Raised when FileResult object is empty
    """
