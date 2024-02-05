# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ContextManager

if TYPE_CHECKING:
    from pyspark.sql import DataFrameReader, DataFrameWriter, SparkSession


class BaseReadableFileFormat(ABC):
    """
    Representation of readable file format.
    """

    @abstractmethod
    def check_if_supported(self, spark: SparkSession) -> None:
        """
        Check if Spark session does support this file format. |support_hooks|

        Raises
        ------
        RuntimeError
            If file format is not supported.
        """

    @abstractmethod
    def apply_to_reader(self, reader: DataFrameReader) -> DataFrameReader | ContextManager[DataFrameReader]:
        """
        Apply provided format to :obj:`pyspark.sql.DataFrameReader`. |support_hooks|

        Returns
        -------
        :obj:`pyspark.sql.DataFrameReader`
            DataFrameReader with options applied.

        ``ContextManager[DataFrameReader]``
            If returned context manager, it will be entered before reading data and exited after creating a DataFrame.
            Context manager's ``__enter__`` method should return :obj:`pyspark.sql.DataFrameReader` instance.
        """


class BaseWritableFileFormat(ABC):
    """
    Representation of writable file format.
    """

    @abstractmethod
    def check_if_supported(self, spark: SparkSession) -> None:
        """
        Check if Spark session does support this file format. |support_hooks|

        Raises
        ------
        RuntimeError
            If file format is not supported.
        """

    @abstractmethod
    def apply_to_writer(self, writer: DataFrameWriter) -> DataFrameWriter | ContextManager[DataFrameWriter]:
        """
        Apply provided format to :obj:`pyspark.sql.DataFrameWriter`. |support_hooks|

        Returns
        -------
        :obj:`pyspark.sql.DataFrameWriter`
            DataFrameWriter with options applied.

        ``ContextManager[DataFrameWriter]``
            If returned context manager, it will be entered before writing and exited after writing a DataFrame.
            Context manager's ``__enter__`` method should return :obj:`pyspark.sql.DataFrameWriter` instance.
        """
