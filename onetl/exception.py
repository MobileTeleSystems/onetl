# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
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
    (e.g. Linux) there are other file types than regular file and directory - symlink, device, etc
    """


class NotAFileError(OSError):
    """
    Like ``NotADirectoryError``, but for files.

    Cannot be replaced with ``FileNotFoundError``, it has different meaning
    """


class FileSizeMismatchError(OSError):
    """
    File size mismatch
    """


class DirectoryExistsError(OSError):
    """
    Like ``FileExistsError``, but for directories.
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


class SparkError(RuntimeError):
    """
    Raised when something went wrong while working with Spark
    """


class TooManyParallelJobsError(SparkError):
    """
    Raised when number parallel jobs is too high
    """


class SignatureError(TypeError):
    """
    Raised when hook signature is not consistent with slot
    """


class TargetAlreadyExistsError(Exception):
    """Raised if the target already exists in source"""
