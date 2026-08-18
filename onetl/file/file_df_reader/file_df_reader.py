# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
import os
import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, ClassVar

from humanize import naturaldelta
from ordered_set import OrderedSet
from pydantic import PrivateAttr, ValidationInfo, field_validator, model_validator

from onetl._util.spark import override_job_description, try_import_pyspark
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
    and parameters, and return a Spark DataFrame. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

    !!! warning

        This class does **not** support read strategies.

    !!! success "Added in 0.9.0"

    Parameters
    ----------
    connection
        File DataFrame connection. See [DBR-onetl-connection-file-df-connection-file-dataframe-connections][] section.

    format
        File format to read.

    source_path
        Directory path to read data from.

        Could be `None`, but only if you pass file paths directly to
        [run][] method

    df_schema
        Spark DataFrame schema.

    options
        Common reading options.

    Examples
    --------

    === "Read CSV files from local filesystem"

        ```python
        from onetl.connection import SparkLocalFS
        from onetl.file import FileDFReader
        from onetl.file.format import CSV

        csv = CSV(delimiter=",")
        local_fs = SparkLocalFS(spark=spark)

        reader = FileDFReader(
            connection=local_fs,
            format=csv,
            source_path="/path/to/directory",
        )
        ```

    === "All supported options"

        ```python
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
        ```
    """

    Options: ClassVar = FileDFReaderOptions

    connection: BaseFileDFConnection
    format: BaseReadableFileFormat
    source_path: PurePathProtocol | None = None
    df_schema: "StructType | None" = None
    options: FileDFReaderOptions = FileDFReaderOptions()

    _connection_checked: bool = PrivateAttr(default=False)

    def __new__(cls, *args, **kwargs):
        try_import_pyspark()

        from pyspark.sql.types import StructType

        _ = StructType

        cls.model_rebuild()
        return super().__new__(cls)

    @slot
    def run(self, files: Iterable[str | os.PathLike] | None = None) -> "DataFrame":
        """
        Method for reading files as DataFrame. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! success "Added in 0.9.0"

        Parameters
        ----------

        files
            File list to read.

            If empty, read files from `source_path`.

        Returns
        -------
        :
            Spark DataFrame

        Examples
        --------

        Read CSV files from directory `/path`:

        ```python
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
        ```
        Read some CSV files using file paths:

        ```python
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
        ```
        Read only specific CSV files in directory:

        ```python
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
        ```
        """

        method = f"{self.__class__.__name__}.run"

        if files is None and not self.source_path:
            msg = f"Cannot call {method}() without files arg or with source_path=None"
            raise ValueError(msg)

        entity_boundary_log(log, f"{method}() started")

        if not self._connection_checked:
            self._log_parameters(files)
            self.connection.check()
            self._connection_checked = True

        if files:
            job_description = f"{self.connection} -> {method}([..files..])"
        else:
            job_description = f"{self.connection} -> {method}({self.source_path})"

        with override_job_description(self.connection.spark, job_description):
            paths: FileSet[PurePathProtocol] = FileSet()
            started = time.perf_counter()
            try:
                if files is not None:
                    paths = FileSet(self._validate_files(files))
                elif self.source_path:
                    paths = FileSet([self.source_path])

                return self._read_files(paths)
            except Exception:
                log.error(  # noqa: TRY400
                    "|%s| Error while reading dataframe",
                    self.__class__.__name__,
                )
                raise
            finally:
                elapsed = naturaldelta(time.perf_counter() - started, minimum_unit="milliseconds")
                entity_boundary_log(log, f"{method}() ended in %s", elapsed, char="-")

    def _read_files(self, paths: FileSet[PurePathProtocol]) -> "DataFrame":
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

        options_dict = self.options.model_dump(exclude_none=True)
        log_options(log, options_dict)

        if files is not None and self.source_path:
            log.warning(
                "|%s| Passed both `source_path` and files list at the same time. Using explicit files list",
                self.__class__.__name__,
            )

    @field_validator("source_path", mode="before")
    @classmethod
    def validate_source_path(cls, value, info: ValidationInfo):
        connection: BaseFileDFConnection | None = info.data.get("connection")
        if not connection or value is None:
            return value
        return connection.path_from_string(value)

    @field_validator("options", mode="before")
    @classmethod
    def _validate_options(cls, value):
        return cls.Options.parse(value)

    @model_validator(mode="after")
    def _validate_format(self):
        self.connection.check_if_format_supported(self.format)
        return self

    def _validate_files(
        self,
        files: Iterable[os.PathLike | str],
    ) -> OrderedSet[PurePathProtocol]:
        result: OrderedSet[PurePathProtocol] = OrderedSet()

        for file in files:
            file_path = file if isinstance(file, PurePathProtocol) else self.connection.path_from_string(file)

            if not self.source_path:
                if not file_path.is_absolute():
                    msg = "Cannot pass relative file path with empty `source_path`"
                    raise ValueError(msg)
            elif file_path.is_absolute() and self.source_path not in file_path.parents:
                msg = f"File path '{file_path}' does not match source_path '{self.source_path}'"
                raise ValueError(msg)
            elif not file_path.is_absolute():
                # Make file path absolute
                file_path = self.source_path / file

            result.add(file_path)

        return result
