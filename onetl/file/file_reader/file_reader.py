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

from __future__ import annotations

import logging
import os
import textwrap
from pathlib import PurePath
from typing import TYPE_CHECKING, Iterable, Optional

from ordered_set import OrderedSet
from pydantic import validator

from onetl.base import BaseFileDFConnection, BaseFileFormat, PurePathProtocol
from onetl.file.file_reader.options import FileReaderOptions
from onetl.file.file_set import FileSet
from onetl.hooks import slot, support_hooks
from onetl.impl import FrozenModel
from onetl.log import (
    entity_boundary_log,
    log_dataframe_schema,
    log_lines,
    log_options,
    log_with_indent,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = logging.getLogger(__name__)


@support_hooks
class FileReader(FrozenModel):
    """Allows you to read files from a source path with specified file connection
    and parameters, and return a Spark DataFrame. |support_hooks|

    .. warning::

        This class does **not** support read strategies.

    Parameters
    ----------
    connection : :obj:`BaseFileDFConnection <onetl.base.base_file_df_connection.BaseFileDFConnection>`
        File DataFrame connection. See :ref:`file-df-connections` section.

    format : :obj:`BaseFileFormat <onetl.base.base_file_format.BaseFileFormat>`
        File format to read.

    source_path : os.PathLike or str, optional, default: ``None``
        Directory path to read data from.

        Could be ``None``, but only if you pass file paths directly to
        :obj:`~run` method

    df_schema : :obj:`pyspark.sql.types.StructType`, optional, default: ``None``
        Spark DataFrame schema.

    options : :obj:`FileReaderOptions <onetl.file.file_reader.options.FileReaderOptions>`, optional
        Common reading options.

    Examples
    --------
    Create reader to parse CSV files in local filesystem:

    .. code:: python

        from onetl.connection import SparkLocalFS
        from onetl.file import FileReader
        from onetl.file.format import CSV

        local_fs = SparkLocalFS(spark=spark)

        reader = FileReader(
            connection=local_fs,
            format=CSV(delimiter=","),
            source_path="/path/to/directory",
        )

    All supported options

    .. code:: python

        from onetl.connection import SparkLocalFS
        from onetl.file import FileReader
        from onetl.file.format import CSV

        csv = CSV(delimiter=",")
        local_fs = SparkLocalFS(spark=spark)

        reader = FileReader(
            connection=local_fs,
            format=csv,
            source_path="/path/to/directory",
            options=FileReader.Options(recursive=False),
        )
    """

    Options = FileReaderOptions

    connection: BaseFileDFConnection
    format: BaseFileFormat
    source_path: Optional[PurePathProtocol] = None
    df_schema: Optional[StructType] = None
    options: FileReaderOptions = FileReaderOptions()

    @slot
    def run(self, files: Iterable[str | os.PathLike] | None = None) -> DataFrame:
        """
        Method for reading files as DataFrame. |support_hooks|

        Parameters
        ----------

        files : Iterator[str | os.PathLike] | None, default ``None``
            File list to read.

            If empty, read files from ``source_path``.

        Returns
        -------
        df : :obj:`pyspark.sql.DataFrame`

            Spark DataFrame

        Examples
        --------

        Read CSV files from directory ``/path``:

        .. code:: python

            from onetl.connection import SparkLocalFS
            from onetl.file import FileReader
            from onetl.file.format import CSV

            csv = CSV(delimiter=",")
            local_fs = SparkLocalFS(spark=spark)

            reader = FileReader(
                connection=local_fs,
                format=csv,
                source_path="/path",
            )
            df = reader.run()

        Read some CSV files using file paths:

        .. code:: python

            from onetl.connection import SparkLocalFS
            from onetl.file import FileReader
            from onetl.file.format import CSV

            csv = CSV(delimiter=",")
            local_fs = SparkLocalFS(spark=spark)

            reader = FileReader(
                connection=local_fs,
                format=csv,
            )

            df = reader.run(
                [
                    "/path/file1.csv",
                    "/path/nested/file2.csv",
                ]
            )

        Read only specific CSV files in directory:

        .. code:: python

            from onetl.connection import SparkLocalFS
            from onetl.file import FileReader
            from onetl.file.format import CSV

            csv = CSV(delimiter=",")
            local_fs = SparkLocalFS(spark=spark)

            reader = FileReader(
                connection=local_fs,
                format=csv,
                source_path="/path",
            )

            df = reader.run(
                [
                    # file paths could be relative
                    "/path/file1.csv",
                    "/path/nested/file2.csv",
                ]
            )
        """

        if files is None and not self.source_path:
            raise ValueError("Neither file list nor `source_path` are passed")

        self._log_parameters(files)

        paths: FileSet[PurePathProtocol] = FileSet()
        if files is not None:
            paths = FileSet(self._validate_files(files))
        elif self.source_path:
            paths = FileSet([self.source_path])

        self.connection.check()
        log_with_indent("")

        df = self._read_files(paths)
        log.info("|%s| DataFrame successfully created", self.__class__.__name__)
        return df

    def _read_files(self, paths: FileSet[PurePathProtocol]) -> DataFrame:
        log.info("|%s| Paths to be read:", self.__class__.__name__)
        log_lines(str(paths))
        log_with_indent("")
        log.info("|%s| Starting the reading process...", self.__class__.__name__)

        return self.connection.read_files_as_df(
            root=self.source_path,
            paths=list(paths),
            format=self.format,
            df_schema=self.df_schema,
            options=self.options,
        )

    def _log_parameters(self, files: Iterable[str | os.PathLike] | None = None) -> None:  # noqa: WPS213
        entity_boundary_log(msg=f"{self.__class__.__name__} starts")

        log.info("|%s| -> |Spark| Reading files using parameters:", self.connection.__class__.__name__)
        log_with_indent("source_path = %s", f"'{self.source_path}'" if self.source_path else "None")
        log_with_indent("format = %r", self.format)

        if self.df_schema:
            empty_df = self.connection.spark.createDataFrame([], self.df_schema)  # type: ignore[attr-defined]
            log_dataframe_schema(empty_df)

        options_dict = self.options.dict(exclude_none=True)
        if options_dict:
            log_options(options_dict)

        if files is not None and self.source_path:
            log.warning(
                "|%s| Passed both `source_path` and files list at the same time. Using explicit files list",
                self.__class__.__name__,
            )

    @validator("source_path", pre=True, always=True)
    def _validate_source_path(cls, source_path, values):
        connection: BaseFileDFConnection = values["connection"]
        if source_path is None:
            return None
        return connection.path_from_string(source_path)

    @validator("format", always=True)
    def _validate_format(cls, format, values):  # noqa: WPS125
        connection: BaseFileDFConnection = values["connection"]
        connection.check_if_format_supported(format)
        return format

    @validator("options")
    def _validate_options(cls, value):
        return cls.Options.parse(value)

    def _validate_files(  # noqa: WPS231
        self,
        files: Iterable[os.PathLike | str],
    ) -> OrderedSet[PurePathProtocol]:
        result: OrderedSet[PurePathProtocol] = OrderedSet()

        for file in files:
            file_path = file if isinstance(file, PurePathProtocol) else PurePath(file)

            if not self.source_path:
                if not file_path.is_absolute():
                    raise ValueError("Cannot pass relative file path with empty `source_path`")
            elif file_path.is_absolute() and self.source_path not in file_path.parents:
                raise ValueError(f"File path '{file_path}' does not match source_path '{self.source_path}'")
            elif not file_path.is_absolute():
                # Make file path absolute
                file_path = self.source_path / file

            result.add(file_path)

        return result

    @classmethod
    def _forward_refs(cls) -> dict[str, type]:
        # avoid importing pyspark unless user called the constructor,
        # as we allow user to use `Connection.package` for creating Spark session

        refs = super()._forward_refs()
        try:
            from pyspark.sql.types import StructType  # noqa: WPS442
        except (ImportError, NameError) as e:
            raise ImportError(
                textwrap.dedent(
                    f"""
                    Cannot import module "pyspark".

                    You should install package as follows:
                        pip install onetl[spark]

                    or inject PySpark to sys.path in some other way BEFORE creating {cls.__name__} instance.
                    """,
                ).strip(),
            ) from e

        refs["StructType"] = StructType
        return refs
