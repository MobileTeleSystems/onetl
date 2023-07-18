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
from typing import TYPE_CHECKING

from onetl.base.base_connection import BaseConnection
from onetl.base.base_file_format import BaseFileFormat
from onetl.base.pure_path_protocol import PurePathProtocol

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, DataFrameReader
    from pyspark.sql.types import StructType


class FileDFReadOptions(ABC):
    """
    Protocol for objects supporting altering Spark DataFrameReader options
    """

    @abstractmethod
    def apply_to_reader(self, reader: DataFrameReader) -> DataFrameReader:
        """
        Apply provided format to :obj:`pyspark.sql.DataFrameReader`.

        Returns
        -------
        :obj:`pyspark.sql.DataFrameReader` with options applied
        """


class BaseFileDFConnection(BaseConnection):
    """
    Implements generic methods for reading  and writing dataframe as files
    """

    @abstractmethod
    def check_if_format_supported(self, format: BaseFileFormat) -> None:  # noqa: WPS125
        """
        Validate if specific file format is supported. |support_hooks|

        Raises
        -------
        RuntimeError
            If file format is not supported.
        """

    @abstractmethod
    def path_from_string(self, path: os.PathLike | str) -> PurePathProtocol:
        """
        Convert path from string to object. |support_hooks|
        """

    @abstractmethod
    def read_files_as_df(
        self,
        paths: list[PurePathProtocol],
        format: BaseFileFormat,  # noqa: WPS125
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
        format: BaseFileFormat,  # noqa: WPS125
    ) -> None:
        """
        Write dataframe as files in some path. |support_hooks|
        """
