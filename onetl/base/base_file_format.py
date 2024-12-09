# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrameReader, DataFrameWriter, SparkSession


class BaseReadableFileFormat(ABC):
    """
    Representation of readable file format.

    .. versionadded:: 0.9.0
    """

    @abstractmethod
    def check_if_supported(self, spark: SparkSession) -> None:
        """
        Check if Spark session does support this file format. |support_hooks|

        .. versionadded:: 0.9.0

        Raises
        ------
        RuntimeError
            If file format is not supported.
        """

    @abstractmethod
    def apply_to_reader(self, reader: DataFrameReader) -> DataFrameReader:
        """
        Apply provided format to :obj:`pyspark.sql.DataFrameReader`. |support_hooks|

        .. versionadded:: 0.9.0

        Returns
        -------
        :obj:`pyspark.sql.DataFrameReader`
            DataFrameReader with options applied.
        """


class BaseWritableFileFormat(ABC):
    """
    Representation of writable file format.

    .. versionadded:: 0.9.0
    """

    @abstractmethod
    def check_if_supported(self, spark: SparkSession) -> None:
        """
        Check if Spark session does support this file format. |support_hooks|

        .. versionadded:: 0.9.0

        Raises
        ------
        RuntimeError
            If file format is not supported.
        """

    @abstractmethod
    def apply_to_writer(self, writer: DataFrameWriter) -> DataFrameWriter:
        """
        Apply provided format to :obj:`pyspark.sql.DataFrameWriter`. |support_hooks|

        .. versionadded:: 0.9.0

        Returns
        -------
        :obj:`pyspark.sql.DataFrameWriter`
            DataFrameWriter with options applied.
        """
