# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import textwrap

from evacuator import NeedEvacuation

MISSING_JVM_CLASS_MSG = textwrap.dedent(
    """
    Cannot import Java class {java_class!r}.

        It looks like you've created Spark session without this option:
            maven_packages = {package_source}.get_packages({args})
            SparkSession.builder.config("spark.jars.packages", ",".join(maven_packages))

        Please call `spark.stop()`, restart the interpreter,
        and then create new SparkSession with proper options.
    """,
).lstrip()


class DirectoryNotFoundError(OSError):
    """
    Like ``FileNotFoundError``, but for directory.

    Cannot be replaced with ``NotAFileError`` because on some operating systems
    (e.g. Linux) there are other file types than regular file and directory - symlink, device, etc.

    .. versionadded:: 0.3.0
    """


class NotAFileError(OSError):
    """
    Like ``NotADirectoryError``, but for files.

    Cannot be replaced with ``FileNotFoundError``, it has different meaning.

    .. versionadded:: 0.3.0
    """


class FileSizeMismatchError(OSError):
    """
    File size mismatch.

    .. versionadded:: 0.8.0
    """


class DirectoryExistsError(OSError):
    """
    Like ``FileExistsError``, but for directories.

    .. versionadded:: 0.8.0
    """


class DirectoryNotEmptyError(OSError):
    """
    Raised when trying to remove directory contains some files or other directories..

    .. versionadded:: 0.3.0
    """


class NoDataError(NeedEvacuation):
    """
    Raised when there is no data in FileResult or DataFrame.

    .. versionadded:: 0.4.0
    """


class FilesError(RuntimeError):
    """
    Raised when something went wrong while working with file collection.

    .. versionadded:: 0.4.0
    """


class SkippedFilesError(FilesError):
    """
    Raised when file collection contains skipped files.

    .. versionadded:: 0.4.0
    """


class FailedFilesError(FilesError):
    """
    Raised when file collection contains failed files.

    .. versionadded:: 0.4.0
    """


class MissingFilesError(FilesError):
    """
    Raised when file collection contains missing files.

    .. versionadded:: 0.4.0
    """


class ZeroFileSizeError(FilesError):
    """
    Raised when file collection contains some zero-sized file.

    .. versionadded:: 0.4.0
    """


class EmptyFilesError(FilesError, NoDataError):
    """
    Raised when file collection is empty.

    .. versionadded:: 0.4.0
    """


class SparkError(RuntimeError):
    """
    Raised when something went wrong while working with Spark.

    .. versionadded:: 0.5.0
    """


class TooManyParallelJobsError(SparkError):
    """
    Raised when number parallel jobs is too high.

    .. versionadded:: 0.5.0
    """


class SignatureError(TypeError):
    """
    Raised when hook signature is not consistent with slot.

    .. versionadded:: 0.7.0
    """


class TargetAlreadyExistsError(Exception):
    """Raised if the target already exists in source.

    .. versionadded:: 0.9.0
    """
