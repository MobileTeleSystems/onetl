# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

from etl_entities.hwm import HWM

from onetl.base.base_connection import BaseConnection
from onetl.hwm import Window

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructField, StructType


class BaseDBDialect(ABC):
    """
    Collection of methods used for validating input values before passing them to read_source_as_df/write_df_to_target
    """

    def __init__(self, connection: "BaseDBConnection") -> None:
        self.connection = connection

    @abstractmethod
    def validate_name(self, value: str) -> str:
        """Check if `source` or `target` value is valid.

        Raises
        ------
        TypeError
            If value type is invalid
        ValueError
            If value is invalid
        """

    @abstractmethod
    def validate_columns(self, columns: str | list[str] | None) -> list[str] | None:
        """Check if `columns` value is valid.

        Raises
        ------
        TypeError
            If value type is invalid
        ValueError
            If value is invalid
        """

    @abstractmethod
    def validate_hwm(self, hwm: HWM | None) -> HWM | None:
        """Check if `HWM` class is valid.

        Raises
        ------
        TypeError
            If hwm type is invalid
        ValueError
            If hwm is invalid
        """

    @abstractmethod
    def validate_df_schema(self, df_schema: "StructType | None") -> "StructType | None":
        """Check if `df_schema` value is valid.

        Raises
        ------
        TypeError
            If value type is invalid
        ValueError
            If value is invalid
        """

    @abstractmethod
    def validate_where(self, where: Any) -> Any | None:
        """Check if `where` value is valid.

        Raises
        ------
        TypeError
            If value type is invalid
        ValueError
            If value is invalid
        """

    @abstractmethod
    def validate_hint(self, hint: Any) -> Any | None:
        """Check if `hint` value is valid.

        Raises
        ------
        TypeError
            If value type is invalid
        ValueError
            If value is invalid
        """

    @abstractmethod
    def detect_hwm_class(self, field: "StructField") -> type[HWM] | None:
        """
        Detects hwm column type based on specific data types in connections data stores
        """


class BaseDBConnection(BaseConnection):
    """
    Implements generic methods for reading and writing dataframe from/to database-like source
    """

    spark: "SparkSession"
    Dialect: ClassVar = BaseDBDialect

    @property
    def dialect(self):
        return self.Dialect(self)

    @property
    @abstractmethod
    def instance_url(self) -> str:
        """Instance URL"""

    # Some implementations may have a different number of parameters.
    # For example, the 'options' parameter may be present. This is fine.
    @abstractmethod
    def read_source_as_df(  # noqa: PLR0913
        self,
        *,
        source: str,
        columns: list[str] | None = None,
        hint: Any | None = None,
        where: Any | None = None,
        df_schema: "StructType | None" = None,
        window: Window | None = None,
        limit: int | None = None,
    ) -> "DataFrame":
        """
        Reads the source to dataframe. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](/hooks/)

        !!! info "Changed in 0.9.0"
            Renamed `read_df` → `read_source_as_df`
        """

    @abstractmethod
    def write_df_to_target(
        self,
        df: "DataFrame",
        target: str,
    ) -> None:
        """
        Saves dataframe to a specific target. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](/hooks/)

        !!! info "Changed in 0.9.0"
            Renamed `write_df` → `write_df_to_target`
        """
