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
from typing import TYPE_CHECKING, Iterable, Optional

from ordered_set import OrderedSet
from pydantic import PrivateAttr, validator

from onetl._util.spark import try_import_pyspark
from onetl.base import BaseFileDFConnection, BaseReadableFileFormat, PurePathProtocol
from onetl.file.file_df_reader.options import FileDFReaderOptions
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
class FileDFReader(FrozenModel):
    """Allows you to read files from a source path with specified file connection
    and parameters, and return a Spark DataFrame. |support_hooks|

    .. warning::

        This class does **not** support read strategies.

    Parameters
    ----------
    connection : :obj:`BaseFileDFConnection <onetl.base.base_file_df_connection.BaseFileDFConnection>`
        File DataFrame connection. See :ref:`file-df-connections` section.

    format : :obj:`BaseReadableFileFormat <onetl.base.base_file_format.BaseReadableFileFormat>`
        File format to read.

    source_path : os.PathLike or str, optional, default: ``None``
        Directory path to read data from.

        Could be ``None``, but only if you pass file paths directly to
        :obj:`~run` method

    df_schema : :obj:`pyspark.sql.types.StructType`, optional, default: ``None``
        Spark DataFrame schema.

    options : :obj:`FileDFReaderOptions <onetl.file.file_df_reader.options.FileDFReaderOptions>`, optional
        Common reading options.

    Examples
    --------
    Create reader to parse CSV files in local filesystem:

    .. code:: python

        from onetl.connection import SparkLocalFS
        from onetl.file import FileDFReader
        from onetl.file.format import CSV

        local_fs = SparkLocalFS(spark=spark)

        reader = FileDFReader(
            connection=local_fs,
            format=CSV(delimiter=","),
            source_path="/path/to/directory",
        )

    All supported options

    .. code:: python

        from onetl.connection import SparkLocalFS
        from onetl.file import FileDFReader
        from onetl.file.format import CSV

        csv = CSV(delimiter=",")
        local_fs = SparkLocalFS(spark=spark)

        reader = FileDFReader(
            connection=local_fs,
            format=csv,
            source_path="/path/to/directory",
            options=FileDFReader.Options(recursive=False),
        )
    """

    Options = FileDFReaderOptions

    connection: BaseFileDFConnection
    format: BaseReadableFileFormat
    source_path: Optional[PurePathProtocol] = None
    df_schema: Optional[StructType] = None
    options: FileDFReaderOptions = FileDFReaderOptions()

    _connection_checked: bool = PrivateAttr(default=False)

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
            from onetl.file import FileDFReader
            from onetl.file.format import CSV

            csv = CSV(delimiter=",")
            local_fs = SparkLocalFS(spark=spark)

            reader = FileDFReader(
                connection=local_fs,
                format=csv,
                source_path="/path",
            )
            df = reader.run()

        Read some CSV files using file paths:

        .. code:: python

            from onetl.connection import SparkLocalFS
            from onetl.file import FileDFReader
            from onetl.file.format import CSV

            csv = CSV(delimiter=",")
            local_fs = SparkLocalFS(spark=spark)

            reader = FileDFReader(
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
            from onetl.file import FileDFReader
            from onetl.file.format import CSV

            csv = CSV(delimiter=",")
            local_fs = SparkLocalFS(spark=spark)

            reader = FileDFReader(
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

        entity_boundary_log(log, msg=f"{self.__class__.__name__}.run() starts")

        if files is None and not self.source_path:
            raise ValueError("Neither file list nor `source_path` are passed")

        if not self._connection_checked:
            self._log_parameters(files)

        paths: FileSet[PurePathProtocol] = FileSet()
        if files is not None:
            paths = FileSet(self._validate_files(files))
        elif self.source_path:
            paths = FileSet([self.source_path])

        if not self._connection_checked:
            self.connection.check()
            log_with_indent(log, "")
            self._connection_checked = True

        df = self._read_files(paths)
        entity_boundary_log(log, msg=f"{self.__class__.__name__}.run() ends", char="-")
        return df

    def _read_files(self, paths: FileSet[PurePathProtocol]) -> DataFrame:
        log.info("|%s| Paths to be read:", self.__class__.__name__)
        log_lines(log, str(paths))
        log_with_indent(log, "")

        return self.connection.read_files_as_df(
            root=self.source_path,
            paths=list(paths),
            format=self.format,
            df_schema=self.df_schema,
            options=self.options,
        )

    def _log_parameters(self, files: Iterable[str | os.PathLike] | None = None) -> None:
        log.info("|%s| -> |Spark| Reading files using parameters:", self.connection.__class__.__name__)
        log_with_indent(log, "source_path = %s", f"'{self.source_path}'" if self.source_path else "None")
        log_with_indent(log, "format = %r", self.format)

        if self.df_schema:
            empty_df = self.connection.spark.createDataFrame([], self.df_schema)  # type: ignore[attr-defined]
            log_dataframe_schema(log, empty_df)

        options_dict = self.options.dict(exclude_none=True)
        log_options(log, options_dict)

        if files is not None and self.source_path:
            log.warning(
                "|%s| Passed both `source_path` and files list at the same time. Using explicit files list",
                self.__class__.__name__,
            )

    @validator("source_path", pre=True)
    def _validate_source_path(cls, source_path, values):
        connection: BaseFileDFConnection = values["connection"]
        if source_path is None:
            return None
        return connection.path_from_string(source_path)

    @validator("format")
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
            file_path = file if isinstance(file, PurePathProtocol) else self.connection.path_from_string(file)

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
        try_import_pyspark()
        from pyspark.sql.types import StructType  # noqa: WPS442

        # avoid importing pyspark unless user called the constructor,
        # as we allow user to use `Connection.get_packages()` for creating Spark session
        refs = super()._forward_refs()
        refs["StructType"] = StructType
        return refs
