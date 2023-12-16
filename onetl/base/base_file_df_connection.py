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

import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ContextManager

from onetl.base.base_connection import BaseConnection
from onetl.base.base_file_format import BaseReadableFileFormat, BaseWritableFileFormat
from onetl.base.pure_path_protocol import PurePathProtocol

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter
    from pyspark.sql.types import StructType


class FileDFReadOptions(ABC):
    """
    Protocol for objects supporting altering Spark DataFrameReader options
    """

    @abstractmethod
    def apply_to_reader(self, reader: DataFrameReader) -> DataFrameReader | ContextManager[DataFrameReader]:
        """
        Apply provided format to :obj:`pyspark.sql.DataFrameReader`.

        Returns
        -------
        :obj:`pyspark.sql.DataFrameReader`
            DataFrameReader with options applied.

        ``ContextManager[DataFrameReader]``
            If returned context manager, it will be entered before reading data and exited after creating a DataFrame.
            Context manager's ``__enter__`` method should return :obj:`pyspark.sql.DataFrameReader` instance.
        """


class FileDFWriteOptions(ABC):
    """
    Protocol for objects supporting altering Spark DataFrameWriter options
    """

    @abstractmethod
    def apply_to_writer(self, writer: DataFrameWriter) -> DataFrameWriter | ContextManager[DataFrameWriter]:
        """
        Apply provided format to :obj:`pyspark.sql.DataFrameWriter`.

        Returns
        -------
        :obj:`pyspark.sql.DataFrameWriter`
            DataFrameWriter with options applied.

        ``ContextManager[DataFrameWriter]``
            If returned context manager, it will be entered before writing and exited after writing a DataFrame.
            Context manager's ``__enter__`` method should return :obj:`pyspark.sql.DataFrameWriter` instance.
        """


class BaseFileDFConnection(BaseConnection):
    """
    Implements generic methods for reading  and writing dataframe as files
    """

    @abstractmethod
    def check_if_format_supported(
        self,
        format: BaseReadableFileFormat | BaseWritableFileFormat,  # noqa: WPS125
    ) -> None:
        """
        Validate if specific file format is supported. |support_hooks|

        Raises
        ------
        RuntimeError
            If file format is not supported.
        """

    @abstractmethod
    def path_from_string(self, path: os.PathLike | str) -> PurePathProtocol:
        """
        Convert path from string to object. |support_hooks|
        """

    @property
    @abstractmethod
    def instance_url(self) -> str:
        """Instance URL"""

    @abstractmethod
    def read_files_as_df(
        self,
        paths: list[PurePathProtocol],
        format: BaseReadableFileFormat,  # noqa: WPS125
        root: PurePathProtocol | None = None,
        df_schema: StructType | None = None,
        options: FileDFReadOptions | None = None,
    ) -> DataFrame:
        """
        Read files in some paths list as dataframe. |support_hooks|
        """

    @abstractmethod
    def write_df_as_files(
        self,
        df: DataFrame,
        path: PurePathProtocol,
        format: BaseWritableFileFormat,  # noqa: WPS125
        options: FileDFWriteOptions | None = None,
    ) -> None:
        """
        Write dataframe as files in some path. |support_hooks|
        """
