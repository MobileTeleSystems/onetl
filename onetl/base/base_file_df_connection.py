# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import os
from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING

from onetl.base.base_connection import BaseConnection
from onetl.base.base_file_format import BaseReadableFileFormat, BaseWritableFileFormat
from onetl.base.pure_path_protocol import PurePathProtocol

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, DataFrameReader, DataFrameWriter, SparkSession
    from pyspark.sql.types import StructType


class FileDFReadOptions(ABC):
    """
    Protocol for objects supporting altering Spark DataFrameReader options.

    !!! success "Added in 0.9.0"
    """

    @abstractmethod
    def apply_to_reader(self, reader: "DataFrameReader") -> "DataFrameReader | AbstractContextManager[DataFrameReader]":
        """
        Apply provided format to `pyspark.sql.DataFrameReader`.

        !!! success "Added in 0.9.0"

        Returns
        -------
        :
            DataFrameReader with options applied.

        contextlib.AbstractContextManager[DataFrameReader]
            If returned context manager, it will be entered before reading data and exited after creating a DataFrame.
            Context manager's `__enter__` method should return `pyspark.sql.DataFrameReader` instance.
        """


class FileDFWriteOptions(ABC):
    """
    Protocol for objects supporting altering Spark DataFrameWriter options.

    !!! success "Added in 0.9.0"
    """

    @abstractmethod
    def apply_to_writer(self, writer: "DataFrameWriter") -> "DataFrameWriter | AbstractContextManager[DataFrameWriter]":
        """
        Apply provided format to `pyspark.sql.DataFrameWriter`.

        !!! success "Added in 0.9.0"

        Returns
        -------
        :
            DataFrameWriter with options applied.

        contextlib.AbstractContextManager[DataFrameWriter]
            If returned context manager, it will be entered before writing and exited after writing a DataFrame.
            Context manager's `__enter__` method should return `pyspark.sql.DataFrameWriter` instance.
        """


class BaseFileDFConnection(BaseConnection):
    """
    Implements generic methods for reading  and writing dataframe as files.

    !!! success "Added in 0.9.0"
    """

    spark: "SparkSession"

    @abstractmethod
    def check_if_format_supported(
        self,
        format: BaseReadableFileFormat | BaseWritableFileFormat,
    ) -> None:
        """
        Validate if specific file format is supported. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! success "Added in 0.9.0"

        Raises
        ------
        RuntimeError
            If file format is not supported.
        """

    @abstractmethod
    def path_from_string(self, path: os.PathLike | str) -> PurePathProtocol:
        """
        Convert path from string to object. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! success "Added in 0.9.0"
        """

    @property
    @abstractmethod
    def instance_url(self) -> str:
        """Instance URL.

        !!! success "Added in 0.9.0"
        """

    @abstractmethod
    def read_files_as_df(
        self,
        paths: list[PurePathProtocol],
        format: BaseReadableFileFormat,
        root: PurePathProtocol | None = None,
        df_schema: "StructType | None" = None,
        options: FileDFReadOptions | None = None,
    ) -> "DataFrame":
        """
        Read files in some paths list as dataframe. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! success "Added in 0.9.0"
        """

    @abstractmethod
    def write_df_as_files(
        self,
        df: "DataFrame",
        path: PurePathProtocol,
        format: BaseWritableFileFormat,
        options: FileDFWriteOptions | None = None,
    ) -> None:
        """
        Write dataframe as files in some path. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! success "Added in 0.9.0"
        """
