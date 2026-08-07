# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
import time
from typing import TYPE_CHECKING, ClassVar

from humanize import naturaldelta
from pydantic import PrivateAttr, ValidationInfo, field_validator, model_validator

from onetl._metrics.command import SparkCommandMetrics
from onetl._metrics.recorder import SparkMetricsRecorder
from onetl._util.spark import override_job_description
from onetl.base import BaseFileDFConnection, BaseWritableFileFormat, PurePathProtocol
from onetl.file.file_df_writer.options import FileDFWriterOptions
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

log = logging.getLogger(__name__)


@support_hooks
class FileDFWriter(FrozenModel):
    """Allows you to write Spark DataFrame as files in a target path of specified file connection
    with parameters. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](/hooks/)

    Parameters
    ----------
    connection : onetl.base.base_file_df_connection.BaseFileDFConnection
        File DataFrame connection. See [file-df-connections][] section.

    format : onetl.base.base_file_format.BaseWritableFileFormat
        File format to write.

    target_path : os.PathLike | str
        Directory path to write data to.

    options : onetl.file.file_df_writer.options.FileDFWriterOptions, optional
        Common writing options.

    Examples
    --------

    === "Write CSV files to local filesystem"

        ```python
        from onetl.connection import SparkLocalFS
        from onetl.file import FileDFWriter
        from onetl.file.format import CSV

        local_fs = SparkLocalFS(spark=spark)

        writer = FileDFWriter(
            connection=local_fs,
            format=CSV(delimiter=","),
            target_path="/path/to/directory",
        )
        ```

    === "All supported options"

        ```python
        from onetl.connection import SparkLocalFS
        from onetl.file import FileDFWriter
        from onetl.file.format import CSV

        csv = CSV(delimiter=",")
        local_fs = SparkLocalFS(spark=spark)

        writer = FileDFWriter(
            connection=local_fs,
            format=csv,
            target_path="/path/to/directory",
            options=FileDFWriter.Options(if_exists="replace_entire_directory"),
        )
        ```
    """

    Options: ClassVar = FileDFWriterOptions

    connection: BaseFileDFConnection
    format: BaseWritableFileFormat
    target_path: PurePathProtocol
    options: FileDFWriterOptions = FileDFWriterOptions()

    _connection_checked: bool = PrivateAttr(default=False)

    @slot
    def run(self, df: "DataFrame") -> None:
        """
        Method for writing DataFrame as files. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](/hooks/)

        !!! note
            Method does support only **batching** DataFrames.

        Parameters
        ----------

        df : pyspark.sql.dataframe.DataFrame
            Spark dataframe

        Examples
        --------

        Write df to target:

        ```python
        writer.run(df)
        ```
        """

        if df.isStreaming:
            msg = f"DataFrame is streaming. {self.__class__.__name__} supports only batch DataFrames."
            raise ValueError(msg)

        method = f"{self.__class__.__name__}.run"
        entity_boundary_log(log, f"{method}() started")

        if not self._connection_checked:
            self._log_parameters(df)
            self.connection.check()
            self._connection_checked = True

        with (
            SparkMetricsRecorder(self.connection.spark) as recorder,
            override_job_description(self.connection.spark, f"{method}({self.target_path}) -> {self.connection}"),
        ):
            started = time.perf_counter()
            try:
                self.connection.write_df_as_files(
                    df=df,
                    path=self.target_path,
                    format=self.format,
                    options=self.options,
                )
            except Exception:
                metrics = recorder.metrics()
                if metrics.output.is_empty:
                    # SparkListener is not a reliable source of information, metrics may or may not be present.
                    # Because of this we also do not return these metrics as method result
                    log.error(  # noqa: TRY400
                        "|%s| Error while writing dataframe.",
                        self.__class__.__name__,
                    )
                else:
                    log.error(  # noqa: TRY400
                        "|%s| Error while writing dataframe. Target MAY contain partially written data!",
                        self.__class__.__name__,
                    )
                self._log_metrics(metrics)
                raise
            else:
                self._log_metrics(recorder.metrics())
            finally:
                elapsed = naturaldelta(time.perf_counter() - started, minimum_unit="milliseconds")
                entity_boundary_log(log, f"{method}() ended in %s", elapsed, char="-")

    def _log_parameters(self, df: "DataFrame") -> None:
        log.info("|Spark| -> |%s| Writing dataframe using parameters:", self.connection.__class__.__name__)
        log_with_indent(log, "target_path = '%s'", self.target_path)
        log_with_indent(log, "format = %r", self.format)

        options_dict = self.options.model_dump(exclude_none=True)
        log_options(log, options_dict)
        log_dataframe_schema(log, df)

    def _log_metrics(self, metrics: SparkCommandMetrics) -> None:
        if not metrics.is_empty:
            log.debug("|%s| Recorded metrics (some values may be missing!):", self.__class__.__name__)
            log_lines(log, str(metrics), level=logging.DEBUG)

    @field_validator("target_path", mode="before")
    @classmethod
    def validate_target_path(cls, value, info: ValidationInfo):
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
