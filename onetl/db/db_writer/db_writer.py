# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

try:
    from pydantic.v1 import Field, PrivateAttr, validator
except (ImportError, AttributeError):
    from pydantic import Field, PrivateAttr, validator  # type: ignore[no-redef, assignment]

from onetl._metrics.command import SparkCommandMetrics
from onetl._metrics.recorder import SparkMetricsRecorder
from onetl._util.spark import override_job_description
from onetl.base import BaseDBConnection
from onetl.hooks import slot, support_hooks
from onetl.impl import FrozenModel, GenericOptions
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
class DBWriter(FrozenModel):
    """Class specifies schema and table where you can write your dataframe. |support_hooks|

    .. versionadded:: 0.1.0

    .. versionchanged:: 0.8.0
        Moved ``onetl.core.DBReader`` → ``onetl.db.DBReader``

    Parameters
    ----------
    connection : :obj:`onetl.connection.DBConnection`
        Class which contains DB connection properties. See :ref:`db-connections` section.

    target : str
        Table/collection/etc name to write data to.

        If connection has schema support, you need to specify the full name of the source
        including the schema, e.g. ``schema.name``.

        .. versionchanged:: 0.7.0
            Renamed ``table`` → ``target``

    options : dict, :obj:`onetl.connection.DBConnection.WriteOptions`, default: ``None``
        Spark write options. Can be in form of special ``WriteOptions`` object or a dict.

        For example:
        ``{"if_exists": "replace_entire_table", "compression": "snappy"}``
        or
        ``Hive.WriteOptions(if_exists="replace_entire_table", compression="snappy")``

        .. note::

            Some sources does not support writing options.


    Examples
    --------

    .. tabs::

        .. code-tab:: py Minimal example

            from onetl.connection import Postgres
            from onetl.db import DBWriter

            postgres = Postgres(...)

            writer = DBWriter(
                connection=postgres,
                target="fiddle.dummy",
            )

        .. code-tab:: py With custom write options

            from onetl.connection import Postgres
            from onetl.db import DBWriter

            postgres = Postgres(...)

            options = Postgres.WriteOptions(if_exists="replace_entire_table", batchsize=1000)

            writer = DBWriter(
                connection=postgres,
                target="fiddle.dummy",
                options=options,
            )
    """

    connection: BaseDBConnection
    target: str = Field(alias="table")
    options: Optional[GenericOptions] = None

    _connection_checked: bool = PrivateAttr(default=False)

    @validator("target", pre=True, always=True)
    def validate_target(cls, target, values):
        connection: BaseDBConnection = values["connection"]
        return connection.dialect.validate_name(target)

    @validator("options", pre=True, always=True)
    def validate_options(cls, options, values):
        connection = values.get("connection")
        write_options_class = getattr(connection, "WriteOptions", None)
        if write_options_class:
            return write_options_class.parse(options)

        if options:
            raise ValueError(
                f"{connection.__class__.__name__} does not implement WriteOptions, but {options!r} is passed",
            )

        return None

    @slot
    def run(self, df: DataFrame) -> None:
        """
        Method for writing your df to specified target. |support_hooks|

        .. note :: Method does support only **batching** DataFrames.

        .. versionadded:: 0.1.0

        Parameters
        ----------
        df : pyspark.sql.dataframe.DataFrame
            Spark dataframe

        Examples
        --------

        Write dataframe to target:

        .. code:: python

            writer.run(df)
        """
        if df.isStreaming:
            raise ValueError(f"DataFrame is streaming. {self.__class__.__name__} supports only batch DataFrames.")

        entity_boundary_log(log, msg=f"{self.__class__.__name__}.run() starts")

        if not self._connection_checked:
            self._log_parameters()
            log_dataframe_schema(log, df)
            self.connection.check()
            self._connection_checked = True

        with SparkMetricsRecorder(self.connection.spark) as recorder:
            try:
                job_description = f"{self.__class__.__name__}.run({self.target}) -> {self.connection}"
                with override_job_description(self.connection.spark, job_description):
                    self.connection.write_df_to_target(
                        df=df,
                        target=str(self.target),
                        **self._get_write_kwargs(),
                    )
            except Exception:
                metrics = recorder.metrics()
                # SparkListener is not a reliable source of information, metrics may or may not be present.
                # Because of this we also do not return these metrics as method result
                if metrics.output.is_empty:
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

        entity_boundary_log(log, msg=f"{self.__class__.__name__}.run() ends", char="-")

    def _log_parameters(self) -> None:
        log.info("|Spark| -> |%s| Writing DataFrame to target using parameters:", self.connection.__class__.__name__)
        log_with_indent(log, "target = '%s'", self.target)

        options = self.options.dict(by_alias=True, exclude_none=True) if self.options else None
        log_options(log, options)

    def _get_write_kwargs(self) -> dict:
        if self.options:
            return {"options": self.options}

        return {}

    def _log_metrics(self, metrics: SparkCommandMetrics) -> None:
        if not metrics.is_empty:
            log.debug("|%s| Recorded metrics (some values may be missing!):", self.__class__.__name__)
            log_lines(log, str(metrics), level=logging.DEBUG)
