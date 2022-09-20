from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Callable

from onetl.base.base_connection import BaseConnection

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType


class BaseDBConnection(BaseConnection):
    """
    Implements generic methods for reading data from and writing data to a database
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
    def get_schema(
        self,
        table: str,
        columns: list[str] | None = None,
    ) -> StructType:
        """
        Get table schema
        """

    @abstractmethod
    def read_table(
        self,
        table: str,
        columns: list[str] | None = None,
        hint: str | None = None,
        where: str | None = None,
    ) -> DataFrame:
        """
        Reads the table to dataframe
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
        hint: str | None = None,
        where: str | None = None,
    ) -> tuple[Any, Any]:
        """
        Get MIN and MAX values for the column
        """

    @abstractmethod
    def expression_with_alias(self, expression: str, alias: str) -> str:
        """
        Return "expression AS alias" statement
        """

    @abstractmethod
    def get_compare_statement(self, comparator: Callable, arg1: Any, arg2: Any) -> str:
        """
        Return "arg1 COMPARATOR arg2" statement
        """
