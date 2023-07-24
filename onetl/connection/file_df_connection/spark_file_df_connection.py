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

import textwrap
from abc import abstractmethod
from contextlib import AbstractContextManager, ExitStack
from logging import getLogger
from typing import TYPE_CHECKING

from pydantic import Field

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
        self._check_if_schema_supported()
        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()
        try:
            path = self._get_spark_default_path()
            fs = self._get_spark_fs()
            fs.getFileStatus(path).isFile()  # type: ignore
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
            log.info("|%s| Reading data ...", self.__class__.__name__)
        else:
            log.info("|%s| Reading data from '%s' ...", self.__class__.__name__, root)

        reader: DataFrameReader = self.spark.read
        with ExitStack() as stack:
            format_result = format.apply_to_reader(reader)
            if isinstance(format_result, AbstractContextManager):
                reader = stack.enter_context(format_result)
            else:
                reader = format_result

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
            format_result = format.apply_to_writer(writer)

            if isinstance(format_result, AbstractContextManager):
                writer = stack.enter_context(format_result)
            else:
                writer = format_result

            if options:
                options_result = options.apply_to_writer(writer)

                if isinstance(options_result, AbstractContextManager):
                    writer = stack.enter_context(options_result)
                else:
                    writer = options_result

            url = self._convert_to_url(path)
            writer.save(url)

        log.info("|%s| Data is successfully saved to '%s'", self.__class__.__name__, path)

    def _check_if_schema_supported(self) -> None:
        """
        Check if filesystem is supported by Spark
        """
        scheme = self._get_spark_default_path().toUri().getScheme()  # type: ignore
        try:
            self._get_spark_fs()
        except Exception:
            msg = f"Spark session does not support filesystem '{scheme}://'.\n{self._installation_instructions}"
            log.error(msg, exc_info=False)
            raise

    @property
    @abstractmethod
    def _installation_instructions(self) -> str:
        """
        Return installation instruction to use in :obj:`~check` method.
        """

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
        path = self._convert_to_url(self._get_default_path())
        jvm = self.spark.sparkContext._jvm
        return jvm.org.apache.hadoop.fs.Path(path)  # type: ignore

    def _get_spark_fs(self):
        """
        Return object of ``org.apache.hadoop.fs.FileSystem`` class for :obj:`~_get_default_path`.
        """
        path = self._get_spark_default_path()
        conf = self.spark.sparkContext._jsc.hadoopConfiguration()  # type: ignore
        return path.getFileSystem(conf)  # type: ignore

    @classmethod
    def _forward_refs(cls) -> dict[str, type]:
        # avoid importing pyspark unless user called the constructor,
        # as we allow user to use `Connection.package` for creating Spark session

        refs = super()._forward_refs()
        try:
            from pyspark.sql import SparkSession  # noqa: WPS442
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

        refs["SparkSession"] = SparkSession
        return refs

    def _log_parameters(self):
        log.info("|Spark| Using connection parameters:")
        log_with_indent("type = %s", self.__class__.__name__)
        parameters = self.dict(by_alias=True, exclude_none=True, exclude={"spark"})
        for attr, value in sorted(parameters.items()):
            log_with_indent("%s = %r", attr, value)
