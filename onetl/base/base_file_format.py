# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrameReader, DataFrameWriter, SparkSession


class BaseReadableFileFormat(ABC):
    """
    Representation of readable file format.

    !!! success "Added in 0.9.0"
    """

    @abstractmethod
    def check_if_supported(self, spark: "SparkSession") -> None:
        """
        Check if Spark session does support this file format. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! success "Added in 0.9.0"

        Raises
        ------
        RuntimeError
            If file format is not supported.
        """

    @abstractmethod
    def apply_to_reader(self, reader: "DataFrameReader") -> "DataFrameReader":
        """
        Apply provided format to `pyspark.sql.DataFrameReader`. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! success "Added in 0.9.0"

        Returns
        -------
        :
            DataFrameReader with options applied.
        """


class BaseWritableFileFormat(ABC):
    """
    Representation of writable file format.

    !!! success "Added in 0.9.0"
    """

    @abstractmethod
    def check_if_supported(self, spark: "SparkSession") -> None:
        """
        Check if Spark session does support this file format. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! success "Added in 0.9.0"

        Raises
        ------
        RuntimeError
            If file format is not supported.
        """

    @abstractmethod
    def apply_to_writer(self, writer: "DataFrameWriter") -> "DataFrameWriter":
        """
        Apply provided format to `pyspark.sql.DataFrameWriter`. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! success "Added in 0.9.0"

        Returns
        -------
        :
            DataFrameWriter with options applied.
        """
