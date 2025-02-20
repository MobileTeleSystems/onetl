# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import abstractmethod
from contextlib import AbstractContextManager, ExitStack
from logging import getLogger
from typing import TYPE_CHECKING

try:
    from pydantic.v1 import Field, validator
except (ImportError, AttributeError):
    from pydantic import Field, validator  # type: ignore[no-redef, assignment]

from onetl._util.hadoop import get_hadoop_config
from onetl._util.spark import override_job_description, try_import_pyspark
from onetl.base import (
    BaseFileDFConnection,
    BaseReadableFileFormat,
    BaseWritableFileFormat,
    FileDFReadOptions,
    FileDFWriteOptions,
    PurePathProtocol,
)
from onetl.hooks import slot, support_hooks
from onetl.impl import FrozenModel
from onetl.log import log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter, SparkSession
    from pyspark.sql.types import StructType

log = getLogger(__name__)


@support_hooks
class SparkFileDFConnection(BaseFileDFConnection, FrozenModel):
    """
    Generic class for any Spark-based FileDFConnection classes.
    """

    spark: SparkSession = Field(repr=False)

    @slot
    def check(self):
        path = self._get_spark_default_path()
        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()

        try:
            with override_job_description(self.spark, f"{self}.check()"):
                fs = self._get_spark_fs()
                fs.exists(path)
            log.info("|%s| Connection is available.", self.__class__.__name__)
        except Exception as e:
            raise RuntimeError("Connection is unavailable") from e

        return self

    def check_if_format_supported(
        self,
        format: BaseReadableFileFormat | BaseWritableFileFormat,  # noqa: WPS125
    ) -> None:
        format.check_if_supported(self.spark)

    @slot
    def read_files_as_df(
        self,
        paths: list[PurePathProtocol],
        format: BaseReadableFileFormat,  # noqa: WPS125
        root: PurePathProtocol | None = None,
        df_schema: StructType | None = None,
        options: FileDFReadOptions | None = None,
    ) -> DataFrame:
        if root:
            log.info("|%s| Reading data from '%s' ...", self.__class__.__name__, root)
        else:
            log.info("|%s| Reading data...", self.__class__.__name__)

        reader: DataFrameReader = self.spark.read
        with ExitStack() as stack:
            reader = format.apply_to_reader(reader)

            if root:
                reader = reader.option("basePath", self._convert_to_url(root))
            if df_schema:
                reader = reader.schema(df_schema)
            if options:
                options_result = options.apply_to_reader(reader)
                if isinstance(options_result, AbstractContextManager):
                    reader = stack.enter_context(options_result)
                else:
                    reader = options_result

            urls = [self._convert_to_url(path) for path in paths]
            df = reader.load(urls)

        log.info("|%s| DataFrame successfully created", self.__class__.__name__)
        return df  # type: ignore

    @slot
    def write_df_as_files(
        self,
        df: DataFrame,
        path: PurePathProtocol,
        format: BaseWritableFileFormat,  # noqa: WPS125
        options: FileDFWriteOptions | None = None,
    ) -> None:
        log.info("|%s| Saving data to '%s' ...", self.__class__.__name__, path)

        writer: DataFrameWriter = df.write
        with ExitStack() as stack:
            writer = format.apply_to_writer(writer)

            if options:
                options_result = options.apply_to_writer(writer)

                if isinstance(options_result, AbstractContextManager):
                    writer = stack.enter_context(options_result)
                else:
                    writer = options_result

            url = self._convert_to_url(path)
            writer.save(url)

        log.info("|%s| Data is successfully saved to '%s'.", self.__class__.__name__, path)

    @abstractmethod
    def _convert_to_url(self, path: PurePathProtocol) -> str:
        """
        Return path with Spark-specific schema prefix, like ``file://``, ``hdfs://``, ``s3a://bucket``.
        """

    @abstractmethod
    def _get_default_path(self) -> PurePathProtocol:
        """
        Return default path.

        Used by :obj:`~check` method to check connection availability.
        """

    def _get_spark_default_path(self):
        """
        Return object of ``org.apache.hadoop.fs.Path`` class for :obj:`~_get_default_path`.
        """
        url = self._convert_to_url(self._get_default_path())
        jvm = self.spark._jvm  # noqa: WPS437
        return jvm.org.apache.hadoop.fs.Path(url)  # type: ignore[union-attr]

    def _get_spark_fs(self):
        """
        Return object of ``org.apache.hadoop.fs.FileSystem`` class for :obj:`~_get_default_path`.
        """
        path = self._get_spark_default_path()
        conf = get_hadoop_config(self.spark)
        return path.getFileSystem(conf)

    @classmethod
    def _forward_refs(cls) -> dict[str, type]:
        try_import_pyspark()

        from pyspark.sql import SparkSession  # noqa: WPS442

        # avoid importing pyspark unless user called the constructor,
        # as we allow user to use `Connection.get_packages()` for creating Spark session
        refs = super()._forward_refs()
        refs["SparkSession"] = SparkSession
        return refs

    @validator("spark")
    def _check_spark_session_alive(cls, spark):
        # https://stackoverflow.com/a/36044685
        msg = "Spark session is stopped. Please recreate Spark session."
        try:
            if not spark._jsc.sc().isStopped():
                return spark
        except Exception as e:
            # None has no attribute "something"
            raise ValueError(msg) from e

        raise ValueError(msg)

    def _log_parameters(self):
        log.info("|%s| Using connection parameters:", self.__class__.__name__)
        parameters = self.dict(exclude_none=True, exclude={"spark"})
        for attr, value in parameters.items():
            log_with_indent(log, "%s = %r", attr, value)
