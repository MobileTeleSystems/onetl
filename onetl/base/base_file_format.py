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

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrameReader, DataFrameWriter, SparkSession


class BaseFileFormat(ABC):
    """
    Representation of file format
    """

    @classmethod
    @abstractmethod
    def check_if_supported(cls, spark: SparkSession) -> None:
        """
        Check if Spark session does support this file format. |support_hooks|

        Raises
        -------
        RuntimeError
            If file format is not supported.
        """

    @abstractmethod
    def apply_to_reader(self, reader: DataFrameReader) -> DataFrameReader:
        """
        Apply provided format to :obj:`pyspark.sql.DataFrameReader`. |support_hooks|

        Returns
        -------
        :obj:`pyspark.sql.DataFrameReader` with options applied
        """

    @abstractmethod
    def apply_to_writer(self, writer: DataFrameWriter) -> DataFrameWriter:
        """
        Apply provided format to :obj:`pyspark.sql.DataFrameWriter`. |support_hooks|

        Returns
        -------
        :obj:`pyspark.sql.DataFrameWriter` with options applied
        """
