# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

try:
    from pydantic.v1 import PrivateAttr, validator
except (ImportError, AttributeError):
    from pydantic import PrivateAttr, validator  # type: ignore[no-redef, assignment]

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
    with parameters. |support_hooks|

    Parameters
    ----------
    connection : :obj:`BaseFileDFConnection <onetl.base.base_file_df_connection.BaseFileDFConnection>`
        File DataFrame connection. See :ref:`file-df-connections` section.

    format : :obj:`BaseWritableFileFormat <onetl.base.base_file_format.BaseWritableFileFormat>`
        File format to write.

    target_path : os.PathLike or str
        Directory path to write data to.

    options : :obj:`FileDFWriterOptions <onetl.file.file_df_writer.options.FileDFWriterOptions>`, optional
        Common writing options.

    Examples
    --------

    .. tabs::

        .. code-tab:: py Write CSV files to local filesystem

            from onetl.connection import SparkLocalFS
            from onetl.file import FileDFWriter
            from onetl.file.format import CSV

            local_fs = SparkLocalFS(spark=spark)

            writer = FileDFWriter(
                connection=local_fs,
                format=CSV(delimiter=","),
                target_path="/path/to/directory",
            )

        .. code-tab:: py All supported options

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
    """

    Options = FileDFWriterOptions

    connection: BaseFileDFConnection
    format: BaseWritableFileFormat
    target_path: PurePathProtocol
    options: FileDFWriterOptions = FileDFWriterOptions()

    _connection_checked: bool = PrivateAttr(default=False)

    @slot
    def run(self, df: DataFrame) -> None:
        """
        Method for writing DataFrame as files. |support_hooks|

        .. note :: Method does support only **batching** DataFrames.

        Parameters
        ----------

        df : pyspark.sql.dataframe.DataFrame
            Spark dataframe

        Examples
        --------

        Write df to target:

        .. code:: python

            writer.run(df)
        """

        entity_boundary_log(log, f"{self.__class__.__name__}.run() starts")

        if df.isStreaming:
            raise ValueError(f"DataFrame is streaming. {self.__class__.__name__} supports only batch DataFrames.")

        if not self._connection_checked:
            self._log_parameters(df)
            self.connection.check()
            self._connection_checked = True

        with SparkMetricsRecorder(self.connection.spark) as recorder:
            try:
                job_description = f"{self.__class__.__name__}.run({self.target_path}) -> {self.connection}"
                with override_job_description(self.connection.spark, job_description):
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
                    log.error(
                        "|%s| Error while writing dataframe.",
                        self.__class__.__name__,
                    )
                else:
                    log.error(
                        "|%s| Error while writing dataframe. Target MAY contain partially written data!",
                        self.__class__.__name__,
                    )
                self._log_metrics(metrics)
                raise
            finally:
                self._log_metrics(recorder.metrics())

        entity_boundary_log(log, f"{self.__class__.__name__}.run() ends", char="-")

    def _log_parameters(self, df: DataFrame) -> None:
        log.info("|Spark| -> |%s| Writing dataframe using parameters:", self.connection.__class__.__name__)
        log_with_indent(log, "target_path = '%s'", self.target_path)
        log_with_indent(log, "format = %r", self.format)

        options_dict = self.options.dict(exclude_none=True)
        log_options(log, options_dict)
        log_dataframe_schema(log, df)

    def _log_metrics(self, metrics: SparkCommandMetrics) -> None:
        if not metrics.is_empty:
            log.debug("|%s| Recorded metrics (some values may be missing!):", self.__class__.__name__)
            log_lines(log, str(metrics), level=logging.DEBUG)

    @validator("target_path", pre=True)
    def _validate_target_path(cls, target_path, values):
        connection: BaseFileDFConnection = values["connection"]
        return connection.path_from_string(target_path)

    @validator("format")
    def _validate_format(cls, format, values):  # noqa: WPS125
        connection: BaseFileDFConnection = values["connection"]
        connection.check_if_format_supported(format)
        return format

    @validator("options")
    def _validate_options(cls, value):
        return cls.Options.parse(value)
