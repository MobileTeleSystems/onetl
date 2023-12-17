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
from typing import TYPE_CHECKING, Any

from onetl.base.base_connection import BaseConnection
from onetl.hwm import Window

if TYPE_CHECKING:
    from etl_entities.hwm import HWM, ColumnHWM
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructField, StructType


class BaseDBDialect(ABC):
    """
    Collection of methods used for validating input values before passing them to read_source_as_df/write_df_to_target
    """

    def __init__(self, connection: BaseDBConnection) -> None:
        self.connection = connection

    @abstractmethod
    def validate_name(self, value: str) -> str:
        """Check if ``source`` or ``target`` value is valid.

        Raises
        ------
        TypeError
            If value type is invalid
        ValueError
            If value is invalid
        """

    @abstractmethod
    def validate_columns(self, columns: list[str] | None) -> list[str] | None:
        """Check if ``columns`` value is valid.

        Raises
        ------
        TypeError
            If value type is invalid
        ValueError
            If value is invalid
        """

    @abstractmethod
    def validate_hwm(self, hwm: HWM | None) -> HWM | None:
        """Check if ``HWM`` class is valid.

        Raises
        ------
        TypeError
            If hwm type is invalid
        ValueError
            If hwm is invalid
        """

    @abstractmethod
    def validate_df_schema(self, df_schema: StructType | None) -> StructType | None:
        """Check if ``df_schema`` value is valid.

        Raises
        ------
        TypeError
            If value type is invalid
        ValueError
            If value is invalid
        """

    @abstractmethod
    def validate_where(self, where: Any) -> Any | None:
        """Check if ``where`` value is valid.

        Raises
        ------
        TypeError
            If value type is invalid
        ValueError
            If value is invalid
        """

    @abstractmethod
    def validate_hint(self, hint: Any) -> Any | None:
        """Check if ``hint`` value is valid.

        Raises
        ------
        TypeError
            If value type is invalid
        ValueError
            If value is invalid
        """

    @abstractmethod
    def detect_hwm_class(self, field: StructField) -> type[ColumnHWM] | None:
        """
        Detects hwm column type based on specific data types in connections data stores
        """


class BaseDBConnection(BaseConnection):
    """
    Implements generic methods for reading and writing dataframe from/to database-like source
    """

    Dialect = BaseDBDialect

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
    def read_source_as_df(
        self,
        source: str,
        columns: list[str] | None = None,
        hint: Any | None = None,
        where: Any | None = None,
        df_schema: StructType | None = None,
        window: Window | None = None,
        limit: int | None = None,
    ) -> DataFrame:
        """
        Reads the source to dataframe. |support_hooks|
        """

    @abstractmethod
    def write_df_to_target(
        self,
        df: DataFrame,
        target: str,
    ) -> None:
        """
        Saves dataframe to a specific target. |support_hooks|
        """
