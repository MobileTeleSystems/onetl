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
from typing import TYPE_CHECKING, Any, Callable

from etl_entities import Table

from onetl.base.base_connection import BaseConnection
from onetl.hwm import Statement

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


class BaseDBConnection(BaseConnection):
    """
    Implements generic methods for reading data from and writing data to a database
    """

    class Dialect(ABC):
        """
        Collection of methods used for validating input values before passing them to read_df/write_df
        """

        @classmethod
        @abstractmethod
        def validate_name(cls, connection: BaseDBConnection, value: Table) -> Table:
            """Check if ``source`` or ``target`` value is valid.

            Raises
            ------
            TypeError
                If value type is invalid
            ValueError
                If value is invalid
            """

        @classmethod
        @abstractmethod
        def validate_columns(cls, connection: BaseDBConnection, columns: list[str] | None) -> list[str] | None:
            """Check if ``columns`` value is valid.

            Raises
            ------
            TypeError
                If value type is invalid
            ValueError
                If value is invalid
            """

        @classmethod
        @abstractmethod
        def validate_df_schema(cls, connection: BaseDBConnection, df_schema: StructType | None) -> StructType | None:
            """Check if ``df_schema`` value is valid.

            Raises
            ------
            TypeError
                If value type is invalid
            ValueError
                If value is invalid
            """

        @classmethod
        @abstractmethod
        def validate_where(cls, connection: BaseDBConnection, where: Any) -> Any | None:
            """Check if ``where`` value is valid.

            Raises
            ------
            TypeError
                If value type is invalid
            ValueError
                If value is invalid
            """

        @classmethod
        @abstractmethod
        def validate_hint(cls, connection: BaseDBConnection, hint: Any) -> Any | None:
            """Check if ``hint`` value is valid.

            Raises
            ------
            TypeError
                If value type is invalid
            ValueError
                If value is invalid
            """

        @classmethod
        @abstractmethod
        def validate_hwm_expression(cls, connection: BaseDBConnection, value: Any) -> str | None:
            """Check if ``hwm_expression`` value is valid.

            Raises
            ------
            TypeError
                If value type is invalid
            ValueError
                If value is invalid
            """

        @classmethod
        @abstractmethod
        def _merge_conditions(cls, conditions: list[Any]) -> Any:
            """
            Convert multiple WHERE conditions to one
            """

        @classmethod
        @abstractmethod
        def _expression_with_alias(cls, expression: Any, alias: str) -> Any:
            """
            Return "expression AS alias" statement
            """

        @classmethod
        @abstractmethod
        def _get_compare_statement(cls, comparator: Callable, arg1: Any, arg2: Any) -> Any:
            """
            Return "arg1 COMPARATOR arg2" statement
            """

    @property
    @abstractmethod
    def instance_url(self) -> str:
        """Instance URL"""

    @abstractmethod
    # Some heirs may have a different number of parameters.
    # For example, the 'options' parameter may be present. This is fine.
    def read_df(
        self,
        source: str,
        columns: list[str] | None = None,
        hint: Any | None = None,
        where: Any | None = None,
        df_schema: StructType | None = None,
        start_from: Statement | None = None,
        end_at: Statement | None = None,
    ) -> DataFrame:
        """
        Reads the source to dataframe. |support_hooks|
        """

    @abstractmethod
    def write_df(
        self,
        df: DataFrame,
        target: str,
    ) -> None:
        """
        Saves dataframe to a specific target. |support_hooks|
        """

    @abstractmethod
    def get_min_max_bounds(
        self,
        source: str,
        column: str,
        expression: str | None = None,
        hint: Any | None = None,
        where: Any | None = None,
    ) -> tuple[Any, Any]:
        """
        Get MIN and MAX values for the column in the source. |support_hooks|
        """
