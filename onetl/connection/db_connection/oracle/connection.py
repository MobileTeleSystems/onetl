# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import os
import re
import warnings
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal
from textwrap import indent
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from onetl._util.spark import get_client_info

try:
    from pydantic.v1 import root_validator
except (ImportError, AttributeError):
    from pydantic import root_validator  # type: ignore[no-redef, assignment]

from etl_entities.instance import Host

from onetl._util.classproperty import classproperty
from onetl._util.version import Version
from onetl.connection.db_connection.jdbc_connection import JDBCConnection
from onetl.connection.db_connection.jdbc_connection.options import JDBCReadOptions
from onetl.connection.db_connection.jdbc_mixin.options import (
    JDBCExecuteOptions,
    JDBCFetchOptions,
)
from onetl.connection.db_connection.oracle.dialect import OracleDialect
from onetl.connection.db_connection.oracle.options import (
    OracleExecuteOptions,
    OracleFetchOptions,
    OracleReadOptions,
    OracleSQLOptions,
    OracleWriteOptions,
)
from onetl.hooks import slot, support_hooks
from onetl.hwm import Window
from onetl.impl import GenericOptions
from onetl.log import BASE_LOG_INDENT, log_lines

# do not import PySpark here, as we allow user to use `Oracle.get_packages()` for creating Spark session
if TYPE_CHECKING:
    from pyspark.sql import DataFrame

log = logging.getLogger(__name__)

# CREATE ... PROCEDURE name ...
# CREATE ... FUNCTION schema.name ...
# CREATE ... PACKAGE "schema"."name" ...
CREATE_DDL_PATTERN = re.compile(
    r"^(?:CREATE|ALTER)\s+.*?"
    r"(?P<type_name>VIEW|PROCEDURE|FUNCTION|PACKAGE|PACKAGE\s+BODY|TRIGGER|TYPE|TYPE\s+BODY|LIBRARY|"
    r"JAVA\s+SOURCE|JAVA\s+CLASS|DIMENSION)\s+"
    r"(?P<full_name>[\w\d._\"]+).*$",
    re.IGNORECASE | re.DOTALL,
)


@dataclass
class ErrorPosition:
    line: int
    position: int
    level: int

    @property
    def sort_key(self) -> tuple[int, int, int]:
        # Sort results by priority (``ERROR``, then ``WARNING``), line, position (symbol)
        return 100 - self.level, self.line, self.position


class OracleExtra(GenericOptions):
    class Config:
        extra = "allow"


@support_hooks
class Oracle(JDBCConnection):
    """Oracle JDBC connection. |support_hooks|

    Based on Maven package `com.oracle.database.jdbc:ojdbc8:23.7.0.25.01 <https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8/23.7.0.25.01>`_
    (`official Oracle JDBC driver <https://www.oracle.com/cis/database/technologies/appdev/jdbc-downloads.html>`_).

    .. seealso::

        Before using this connector please take into account :ref:`oracle-prerequisites`

    .. versionadded:: 0.1.0

    Parameters
    ----------
    host : str
        Host of Oracle database. For example: ``test.oracle.domain.com`` or ``193.168.1.10``

    port : int, default: ``1521``
        Port of Oracle database

    user : str
        User, which have proper access to the database. For example: ``SOME_USER``

    password : str
        Password for database connection

    sid : str, default: ``None``
        Sid of oracle database. For example: ``XE``

        .. warning ::

            You should provide either ``sid`` or ``service_name``, not both of them

    service_name : str, default: ``None``
        Specifies one or more names by which clients can connect to the instance.

        For example: ``PDB1``.

        .. warning ::

            You should provide either ``sid`` or ``service_name``, not both of them

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session.

    extra : dict, default: ``None``
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"remarksReporting": "false"}``

        See official documentation:
            * `Connection parameters <https://docs.oracle.com/en/database/oracle/oracle-database/23/jajdb/oracle/jdbc/OracleDriver.html>`_
            * `Connection properties <https://docs.oracle.com/cd/A97335_02/apps.102/a83724/basic1.htm#1024018>`_

    Examples
    --------

    Create and check Oracle connection with ``sid``:

    .. code:: python

        from onetl.connection import Oracle
        from pyspark.sql import SparkSession

        # Create Spark session with Oracle driver loaded
        maven_packages = Oracle.get_packages()
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        # Create connection
        oracle = Oracle(
            host="database.host.or.ip",
            user="user",
            password="*****",
            sid="XE",
            extra={"remarksReporting": "false"},
            spark=spark,
        ).check()

    or with ``service_name``:

    .. code:: python

        ...

        oracle = Oracle(
            host="database.host.or.ip",
            user="user",
            password="*****",
            service_name="PDB1",  # <--- instead of SID
            extra={"remarksReporting": "false"},
            spark=spark,
        ).check()

    """

    host: Host
    port: int = 1521
    sid: Optional[str] = None
    service_name: Optional[str] = None
    extra: OracleExtra = OracleExtra()

    ReadOptions = OracleReadOptions
    WriteOptions = OracleWriteOptions
    SQLOptions = OracleSQLOptions
    FetchOptions = OracleFetchOptions
    ExecuteOptions = OracleExecuteOptions

    Extra = OracleExtra
    Dialect = OracleDialect

    DRIVER: ClassVar[str] = "oracle.jdbc.driver.OracleDriver"
    _CHECK_QUERY: ClassVar[str] = "SELECT 1 FROM dual"

    @slot
    @classmethod
    def get_packages(
        cls,
        java_version: str | None = None,
        package_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. Allows specifying custom JDBC driver versions for Oracle. |support_hooks|

        Parameters
        ----------
        java_version : str, optional
            Java major version, defaults to "8". Must be "8" or "11".
        package_version : str, optional
            Specifies the version of the Oracle JDBC driver to use. Defaults to "23.7.0.25.01".

        Examples
        --------

        .. code:: python

            from onetl.connection import Oracle

            Oracle.get_packages()

            # specify Java and package versions
            Oracle.get_packages(java_version="8", package_version="23.7.0.25.01")
        """

        default_java_version = "8"
        default_package_version = "23.7.0.25.01"

        java_ver = Version(java_version or default_java_version)
        if java_ver.major < 8:
            raise ValueError(f"Java version must be at least 8, got {java_ver.major}")

        jre_ver = "8" if java_ver.major < 11 else "11"
        jdbc_version = Version(package_version or default_package_version).min_digits(4)

        return [f"com.oracle.database.jdbc:ojdbc{jre_ver}:{jdbc_version}"]

    @classproperty
    def package(cls) -> str:
        """Get package name to be downloaded by Spark."""
        msg = "`Oracle.package` will be removed in 1.0.0, use `Oracle.get_packages()` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "com.oracle.database.jdbc:ojdbc8:23.7.0.25.01"

    @property
    def jdbc_url(self) -> str:
        if self.sid:
            return f"jdbc:oracle:thin:@{self.host}:{self.port}:{self.sid}"

        return f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service_name}"

    @property
    def jdbc_params(self) -> dict:
        result = super().jdbc_params
        result.update(self.extra.dict(by_alias=True))
        # https://stackoverflow.com/questions/35072134/why-am-i-getting-format-error-property-is-vsession-program-connecting-to-o/35072449#35072449
        result["v$session.program"] = result.get("v$session.program", get_client_info(self.spark, limit=48))
        return result

    @property
    def instance_url(self) -> str:
        if self.sid:
            return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}/{self.sid}"

        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}/{self.service_name}"

    def __str__(self):
        if self.sid:
            return f"{self.__class__.__name__}[{self.host}:{self.port}/{self.sid}]"

        return f"{self.__class__.__name__}[{self.host}:{self.port}/{self.service_name}]"

    @slot
    def get_min_max_values(
        self,
        source: str,
        window: Window,
        hint: Any | None = None,
        where: Any | None = None,
        options: JDBCReadOptions | None = None,
    ) -> tuple[Any, Any]:
        min_value, max_value = super().get_min_max_values(
            source=source,
            window=window,
            hint=hint,
            where=where,
            options=options,
        )
        # Oracle does not have Integer type, only Numeric, which is represented as Decimal in Python
        # If number does not have decimal part, convert it to integer to use as lowerBound/upperBound
        if isinstance(min_value, Decimal) and min_value == round(min_value):
            min_value = int(min_value)
        if isinstance(max_value, Decimal) and max_value == round(max_value):
            max_value = int(max_value)
        return min_value, max_value

    @root_validator
    def _only_one_of_sid_or_service_name(cls, values):
        sid = values.get("sid")
        service_name = values.get("service_name")

        if sid and service_name:
            raise ValueError("Only one of parameters ``sid``, ``service_name`` can be set, got both")

        if not sid and not service_name:
            raise ValueError("One of parameters ``sid``, ``service_name`` should be set, got none")

        return values

    def _call_on_driver(
        self,
        query: str,
        options: JDBCExecuteOptions,
    ) -> DataFrame | None:
        result = super()._call_on_driver(query, options)
        self._handle_compile_errors(query.strip(), options)
        return result

    def _parse_create_statement(self, statement: str) -> tuple[str, str, str] | None:
        """
        Parses ``CREATE ... type_name [schema.]object_name ...`` statement
        and return parsed values, or ``None``.

        ``type_name`` is something like ``PROCEDURE``, ``FUNCTION``, ``PACKAGE``.

        If ``schema`` is not set implicitly, it is replaced by current user name.
        """

        match = CREATE_DDL_PATTERN.match(statement)
        if not match:
            return None

        # get latest error
        type_name = match.group("type_name")
        full_name = match.group("full_name")
        schema, _, object_name = full_name.partition(".")

        object_name = object_name.strip().strip('"')
        schema = schema.strip().strip('"') or self.user

        return type_name, schema, object_name

    def _get_compile_errors(
        self,
        type_name: str,
        schema: str,
        object_name: str,
        options: JDBCExecuteOptions | JDBCFetchOptions,
    ) -> list[tuple[ErrorPosition, str]]:
        """
        Get compile errors for the object.

        See Oracle documentation about ``ALL_ERRORS`` VIEW:
        https://docs.oracle.com/cd/B19306_01/server.102/b14237/statviews_1052.htm#i1577005
        """

        show_errors = f"""
            SELECT *
            FROM ALL_ERRORS
            WHERE
                UPPER(OWNER) = '{schema.upper()}' AND
                UPPER(NAME) = '{object_name.upper()}' AND
                UPPER(TYPE) = '{type_name.upper()}'
            ORDER BY
                SEQUENCE,
                LINE,
                POSITION
        """
        errors = self._query_on_driver(show_errors, options).collect()
        if not errors:
            return []

        result: list[tuple[ErrorPosition, str]] = []
        for error in errors:
            level = logging.getLevelName(error.ATTRIBUTE)
            line = int(error.LINE)
            position = int(error.POSITION)
            text = error.TEXT
            result.append((ErrorPosition(line, position, level), text))

        return result

    def _aggregate_compile_errors(self, raw_errors: list[tuple[ErrorPosition, str]]) -> OrderedDict[ErrorPosition, str]:
        """
        If error message is large, its lines are stored as different rows
        in ``ALL_ERRORS`` view in with same line+position+level.

        This method merges these lines back into one text message.
        """

        errors = sorted(raw_errors, key=lambda item: item[0].sort_key)

        aggregated_messages: OrderedDict[ErrorPosition, str] = OrderedDict()
        for error_position, text in errors:
            message = aggregated_messages.get(error_position, "")
            message += text
            aggregated_messages[error_position] = message.rstrip()

        return aggregated_messages

    def _build_error_message(self, aggregated_errors: OrderedDict[ErrorPosition, str]) -> str:
        """
        Build compile errors into human-readable message.
        """

        error_lines: list[str] = []
        for error, messages in aggregated_errors.items():
            level_name = logging.getLevelName(error.level)
            prefix = f"[{level_name}] Line {error.line}, position {error.position}:"
            text = os.linesep + messages.strip()
            error_lines.append(prefix + indent(text, " " * BASE_LOG_INDENT))

        return os.linesep.join(error_lines)

    def _handle_compile_errors(
        self,
        statement: str,
        options: JDBCExecuteOptions,
    ) -> None:
        """
        Oracle does not return compilation errors immediately.
        Instead, user should call "SHOW ERRORS" statement to get the message. But it cannot be called via JDBC.

        So this method is fetching errors from the system views:
        1. ``SELECT * FROM ALL_ERRORS``
        2. Parse resulting dataframe into list of compilation errors
        3. Generate error message from errors list
        4. If there are records with ``ERROR`` level, method throws ValueError, otherwise prints warning to log
        """

        parsed_statement = self._parse_create_statement(statement)
        if not parsed_statement:
            return

        type_name, schema, object_name = parsed_statement

        raw_errors = self._get_compile_errors(type_name, schema, object_name, options)
        if not raw_errors:
            return

        aggregated_errors = self._aggregate_compile_errors(raw_errors)
        fail = any(error.level == logging.ERROR for error in aggregated_errors)

        message = self._build_error_message(aggregated_errors)
        log_lines(log, message, level=logging.ERROR if fail else logging.WARNING)

        if fail:
            raise ValueError(message)
