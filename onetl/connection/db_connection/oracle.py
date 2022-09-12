from __future__ import annotations

import logging
import re
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime
from textwrap import indent
from typing import TYPE_CHECKING, ClassVar, Optional

from pydantic import root_validator

from onetl._internal import clear_statement  # noqa: WPS436
from onetl.connection.db_connection.jdbc_connection import JDBCConnection
from onetl.log import BASE_LOG_INDENT, log_with_indent

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


class Oracle(JDBCConnection):
    """Class for Oracle JDBC connection.

    Based on Maven package ``com.oracle.database.jdbc:ojdbc8:21.6.0.0.1``
    (`official Oracle JDBC driver <https://www.oracle.com/cis/database/technologies/appdev/jdbc-downloads.html>`_)

    .. note::

        Supported Oracle Server versions: 21c, 19c, 18c, and 12.2

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

            Be careful, to correct work you must provide ``sid`` or ``service_name``

    service_name : str, default: ``None``
        Specifies one or more names by which clients can connect to the instance.

        For example: ``MYDATA``.

        .. warning ::

            Be careful, for correct work you must provide ``sid`` or ``service_name``

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session.

        You can use ``mtspark`` for spark session initialization

    extra : dict, default: ``None``
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"defaultBatchValue": 100}``

        See `Oracle JDBC driver properties documentation
        <https://docs.oracle.com/cd/E11882_01/appdev.112/e13995/oracle/jdbc/OracleDriver.html>`_
        for more details

    Examples
    --------

    Oracle connection initialization

    .. code::

        from onetl.connection import Oracle
        from mtspark import get_spark

        extra = {"defaultBatchValue": 100}

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": [
                "default:skip",
                Oracle.package,
            ],
        })

        oracle = Oracle(
            host="database.host.or.ip",
            user="user",
            password="*****",
            sid='XE',
            extra=extra,
            spark=spark,
        )

    """

    port: int = 1521
    sid: Optional[str] = None
    service_name: Optional[str] = None

    driver: ClassVar[str] = "oracle.jdbc.driver.OracleDriver"
    package: ClassVar[str] = "com.oracle.database.jdbc:ojdbc8:21.6.0.0.1"

    _check_query: ClassVar[str] = "SELECT 1 FROM dual"

    @root_validator
    def only_one_of_sid_or_service_name(cls, values):  # noqa: N805
        sid = values.get("sid")
        service_name = values.get("service_name")

        if sid and service_name:
            raise ValueError("Only one of parameters ``sid``, ``service_name`` can be set, got both")

        if not sid and not service_name:
            raise ValueError("One of parameters ``sid``, ``service_name`` should be set, got none")

        return values

    class ReadOptions(JDBCConnection.ReadOptions):
        @classmethod
        def _get_partition_column_hash(cls, partition_column: str, num_partitions: int) -> str:
            return f"ora_hash({partition_column}, {num_partitions})"

        @classmethod
        def _get_partition_column_mod(cls, partition_column: str, num_partitions: int) -> str:
            return f"MOD({partition_column}, {num_partitions})"

    @property
    def jdbc_url(self) -> str:
        params_str = "&".join(f"{k}={v}" for k, v in sorted(self.extra.dict(by_alias=True).items()))

        if params_str:
            params_str = f"?{params_str}"

        if self.sid:
            return f"jdbc:oracle:thin:@{self.host}:{self.port}:{self.sid}{params_str}"

        return f"jdbc:oracle:thin:@//{self.host}:{self.port}/{self.service_name}{params_str}"

    @property
    def instance_url(self) -> str:
        if self.sid:
            return f"{super().instance_url}/{self.sid}"

        return f"{super().instance_url}/{self.service_name}"

    def execute(
        self,
        statement: str,
        options: Oracle.JDBCOptions | dict | None = None,  # noqa: WPS437
    ) -> DataFrame | None:

        statement = clear_statement(statement)

        log.info(f"|{self.__class__.__name__}| Executing statement (on driver):")
        log_with_indent(statement)

        call_options = self.JDBCOptions.parse(options)
        df = self._call_on_driver(statement, call_options)
        self._handle_compile_errors(statement.lower().strip(), call_options)

        message = f"|{self.__class__.__name__}| Execution succeeded"
        if df is not None:
            rows_count = df.count()
            message += f", resulting dataframe contains {rows_count} rows"

        log.info(message)
        return df

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
        options: Oracle.JDBCOptions,
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
            text = "\n" + messages.strip()
            error_lines.append(prefix + indent(text, " " * BASE_LOG_INDENT))

        return "\n".join(error_lines)

    def _handle_compile_errors(
        self,
        statement: str,
        options: Oracle.JDBCOptions,
    ) -> None:
        """
        Oracle does not return compilation errors immediately.
        Instead, user should call "SHOW ERRORS" statement to get the message. But it cannot be called via JDBC.

        So this method is fetching errors from the system views:
        1. ``SELECT * FROM ALL_ERRORS``
        2. Parse resulting dataframe into list of compilation errors
        3. Generage error message from errors list
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

        log_with_indent(message, level=logging.ERROR if fail else logging.WARNING)

        if fail:
            raise ValueError(message)

    def _get_datetime_value_sql(self, value: datetime) -> str:
        result = value.strftime("%Y-%m-%d %H:%M:%S")
        return f"TO_DATE('{result}', 'YYYY-MM-DD HH24:MI:SS')"

    def _get_date_value_sql(self, value: date) -> str:
        result = value.strftime("%Y-%m-%d")
        return f"TO_DATE('{result}', 'YYYY-MM-DD')"
