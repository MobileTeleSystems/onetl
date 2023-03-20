#  Copyright 2022 MTS (Mobile Telesystems)
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
        Collection of methods used for validating input values before passing them to read_table/save_df
        """

        @classmethod
        @abstractmethod
        def validate_columns(cls, connection: BaseDBConnection, columns: Any) -> list[str] | None:
            ...

        @classmethod
        @abstractmethod
        def validate_df_schema(cls, connection: BaseDBConnection, df_schema: StructType | None) -> StructType | None:
            ...

        @classmethod
        @abstractmethod
        def validate_where(cls, connection: BaseDBConnection, where: Any) -> Any | None:
            ...

        @classmethod
        @abstractmethod
        def validate_hint(cls, connection: BaseDBConnection, hint: Any) -> Any | None:
            ...

        @classmethod
        @abstractmethod
        def _validate_hwm_expression(cls, connection: BaseDBConnection, value: Any) -> str | None:
            """
            You can't pass the hwm_strategy parameter to MongoDB
            """
            ...

        @classmethod
        @abstractmethod
        def _where_condition(cls, result: list[Any]) -> Any:
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

        @classmethod
        @abstractmethod
        def _validate_table(cls, connection: BaseDBConnection, value: Table) -> Table:
            """
            Returns "db_schema.table".
            """

    @property
    @abstractmethod
    def instance_url(self) -> str:
        """Instance URL"""

    @abstractmethod
    def check(self):
        """
        Check if database is accessible.

        If not, an exception will be raised.

        Executes some simple query, like ``SELECT 1``, in the database.

        Examples
        --------

        Database is online:

        .. code:: python

            connection.check()

        Database is offline or not accessible:

        .. code:: python

            connection.check()  # raises RuntimeError exception
        """

    @abstractmethod
    # Some heirs may have a different number of parameters.
    # For example, the 'options' parameter may be present. This is fine.
    def read_table(
        self,
        table: str,
        columns: list[str] | None = None,
        hint: Any | None = None,
        where: Any | None = None,
        df_schema: StructType | None = None,
        start_from: Statement | None = None,
        end_at: Statement | None = None,
    ) -> DataFrame:
        """
        Reads the table to dataframe.
        """

    @abstractmethod
    def save_df(
        self,
        df: DataFrame,
        table: str,
    ) -> None:
        """
        Saves dataframe to a specific table
        """

    @abstractmethod
    def get_min_max_bounds(
        self,
        table: str,
        column: str,
        expression: str | None = None,
        hint: Any | None = None,
        where: Any | None = None,
    ) -> tuple[Any, Any]:
        """
        Get MIN and MAX values for the column
        """
