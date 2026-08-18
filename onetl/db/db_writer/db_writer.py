# SPDX-FileCopyrightText: 2021-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
import time
from typing import TYPE_CHECKING

from humanize import naturaldelta
from pydantic import Field, PrivateAttr, ValidationInfo, field_validator

from onetl._metrics.command import SparkCommandMetrics
from onetl._metrics.recorder import SparkMetricsRecorder
from onetl._util.alias import avoid_alias
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
    """Class specifies schema and table where you can write your dataframe. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

    !!! success "Added in 0.1.0"

    !!! info "Changed in 0.8.0"
        Moved `onetl.core.DBReader` → `onetl.db.DBReader`

    Parameters
    ----------
    connection
        Class which contains DB connection properties. See [DBR-onetl-connection-db-connection-db-connections][] section.

    target
        Table/collection/etc name to write data to.

        If connection has schema support, you need to specify the full name of the source
        including the schema, e.g. `schema.name`.

        !!! info "Changed in 0.7.0"
            Renamed `table` → `target`

    options
        Spark write options. Can be in form of special `WriteOptions` object or a dict.

        For example:
        `{"if_exists": "replace_entire_table", "compression": "snappy"}`
        or
        `Hive.WriteOptions(if_exists="replace_entire_table", compression="snappy")`

        !!! note

            Some sources does not support writing options.


    Examples
    --------

    === "Minimal example"

        ```python
        from onetl.connection import Postgres
        from onetl.db import DBWriter

        postgres = Postgres(...)

        writer = DBWriter(
            connection=postgres,
            target="fiddle.dummy",
        )
        ```

    === "With custom write options"

        ```python
        from onetl.connection import Postgres
        from onetl.db import DBWriter

        postgres = Postgres(...)

        options = Postgres.WriteOptions(if_exists="replace_entire_table", batchsize=1000)

        writer = DBWriter(
            connection=postgres,
            target="fiddle.dummy",
            options=options,
        )
        ```
    """

    connection: BaseDBConnection
    target: str = Field(alias=avoid_alias("table"))  # type: ignore[literal-required]
    options: GenericOptions | None = None

    _connection_checked: bool = PrivateAttr(default=False)

    @field_validator("target", mode="before")
    @classmethod
    def _validate_target(cls, value, info: ValidationInfo):
        connection: BaseDBConnection | None = info.data.get("connection")
        if not connection:
            return value
        return connection.dialect.validate_name(value)

    @field_validator("options", mode="before")
    @classmethod
    def _validate_options(cls, value, info: ValidationInfo):
        connection: BaseDBConnection | None = info.data.get("connection")
        if not connection:
            return value

        write_options_class = getattr(connection, "WriteOptions", None)
        if write_options_class:
            return write_options_class.parse(value)

        if not value:
            return None

        msg = f"{connection.__class__.__name__} does not implement WriteOptions, but {value!r} is passed"
        raise ValueError(msg)

    @slot
    def run(self, df: "DataFrame") -> None:
        """
        Method for writing your df to specified target. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! note
            Method does support only **batching** DataFrames.

        !!! success "Added in 0.1.0"

        Parameters
        ----------
        df
            Spark dataframe

        Examples
        --------

        Write dataframe to target:

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
            self._log_parameters()
            log_dataframe_schema(log, df)
            self.connection.check()
            self._connection_checked = True

        with (
            SparkMetricsRecorder(self.connection.spark) as recorder,
            override_job_description(self.connection.spark, f"{method}({self.target}) -> {self.connection}"),
        ):
            started = time.perf_counter()
            try:
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
                    log.error(  # noqa: TRY400
                        "|%s| Error while writing dataframe",
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

    def _log_parameters(self) -> None:
        log.info("|Spark| -> |%s| Writing DataFrame to target using parameters:", self.connection.__class__.__name__)
        log_with_indent(log, "target = '%s'", self.target)

        options = self.options.model_dump(by_alias=True, exclude_none=True) if self.options else None
        log_options(log, options)

    def _get_write_kwargs(self) -> dict:
        if self.options:
            return {"options": self.options}

        return {}

    def _log_metrics(self, metrics: SparkCommandMetrics) -> None:
        if not metrics.is_empty:
            log.debug("|%s| Recorded metrics (some values may be missing!):", self.__class__.__name__)
            log_lines(log, str(metrics), level=logging.DEBUG)
