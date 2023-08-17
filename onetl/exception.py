#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

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
