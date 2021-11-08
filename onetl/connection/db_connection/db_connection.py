from abc import abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from logging import getLogger
import operator
from typing import Any, Callable, ClassVar, Optional, Dict

from onetl.connection import ConnectionABC
from onetl.mixins.options_validator import JDBCParamsCreatorMixin

log = getLogger(__name__)


@dataclass(frozen=True)
class DBConnection(ConnectionABC, JDBCParamsCreatorMixin):
    driver: str = field(init=False, default="")
    host: str = ""
    port: Optional[int] = None
    user: Optional[str] = None
    password: Optional[str] = field(repr=False, default=None)
    # Database in rdbms, schema in DBReader.
    # Difference like https://www.educba.com/postgresql-database-vs-schema/
    database: str = "default"
    extra: Dict = field(default_factory=dict)
    # TODO:(@dypedchenk) Create abstract class for engine. Engine uses pyhive session or Engine uses pyspark session
    spark: Optional["pyspark.sql.SparkSession"] = None

    compare_statements: ClassVar[Dict[Callable, str]] = {
        operator.ge: "{} >= {}",
        operator.gt: "{} > {}",
        operator.le: "{} <= {}",
        operator.lt: "{} < {}",
        operator.eq: "{} == {}",
        operator.ne: "{} != {}",
    }

    def read_table(
        self,
        jdbc_options: Dict,
        table: str,
        columns: Optional[str],
        hint: Optional[str],
        where: Optional[str],
    ) -> "pyspark.sql.DataFrame":

        # TODO:(@dypedchenk) Here will need to implement a hint in case of typos in writing parameters
        if not self.spark:
            raise ValueError("Spark session not provided")

        if not jdbc_options.get("fetchsize"):
            log.debug("<fetchsize> task parameter wasn't specified; the reading will be slowed down!")

        if jdbc_options.get("sessionInitStatement"):
            log.debug(f"Init SQL statement: {jdbc_options.get('sessionInitStatement')}")

        sql_text = self.get_sql_query(
            table=table,
            hint=hint,
            columns=columns,
            where=where,
        )

        log.info(f"{self.__class__.__name__}: Reading table {table}")
        log.info(f"{self.__class__.__name__}: SQL statement: {sql_text}")

        options = self.jdbc_params_creator(jdbc_options=jdbc_options)
        options = self.set_lower_upper_bound(jdbc_options=options, table=table)

        log.debug(
            f"USER='{self.user}' " f"OPTIONS={options} DRIVER={options['properties']['driver']}",
        )
        log.debug(f"JDBC_URL='{self.url}'")

        # for convenience. parameters accepted by spark.read.jdbc method
        #  spark.read.jdbc(
        #    url, table, column, lowerBound, upperBound, numPartitions, predicates
        #    properties:  { "user" : "SYSTEM", "password" : "mypassword", ... })

        return self.spark.read.jdbc(table=f"({sql_text}) T", **options)

    def save_df(
        self,
        df: "pyspark.sql.DataFrame",
        table: str,
        jdbc_options: Dict,
    ) -> None:
        """
        Save the DataFrame into RDB.

        """

        options = jdbc_options.copy()
        options = self.jdbc_params_creator(jdbc_options=options)
        mode = options.pop("mode")
        format = options.pop("format")  # noqa: WPS125

        log_pass = "PASSWORD='*****'" if options["properties"].get("password") else "NO_PASSWORD"

        log.debug(f"USER='{self.user}' {log_pass} DRIVER={options['properties']['driver']}")
        log.debug(f"JDBC_URL='{self.url}'")

        # for convenience. parameters accepted by spark.write.jdbc method
        #   spark.read.jdbc(
        #     url, table, mode,
        #     properties:  { "user" : "SYSTEM", "password" : "mypassword", ... })

        df.write.mode(mode).format(format).jdbc(table=table, **options)

    @staticmethod
    def get_sql_query(
        table: str,
        hint: Optional[str] = None,
        columns: Optional[str] = "*",
        where: Optional[str] = None,
    ) -> str:
        """
        Creates a sql query for the dbtable parameter in the jdbc function.
        For instance: spark.read.jdbc(dbtable=sql)
        """
        hint = f"/*+ {hint} */" if hint else None
        where = f"WHERE {where}" if where else None

        statements = [
            "SELECT",
            hint,
            columns,
            f"FROM {table}",
            where,
        ]

        # The elements of the array with the value None are removed
        state: list = [x for x in statements if x]
        return " ".join(state)

    def raw_query(self, sql: str, jdbc_options: Dict) -> "pyspark.sql.DataFrame":
        jdbc_options_raw = jdbc_options.copy()
        jdbc_options_raw["table"] = sql
        jdbc_options_raw.pop("numPartitions", None)
        jdbc_options_raw.pop("lowerBound", None)
        jdbc_options_raw.pop("upperBound", None)
        jdbc_options_raw.pop("column", None)
        return self.spark.read.jdbc(**jdbc_options_raw)  # type: ignore[union-attr]

    def get_schema(self, table: str, columns: str, jdbc_options: Dict) -> "pyspark.sql.types.StructType":
        jdbc_options = self.jdbc_params_creator(jdbc_options=jdbc_options)

        query_schema = f"(SELECT {columns} FROM {table} WHERE 1 = 0) T"
        jdbc_options["table"] = query_schema
        jdbc_options["properties"]["fetchsize"] = "0"

        log.info(f"{self.__class__.__name__}: Fetching table {table} schema")
        log.info(f"{self.__class__.__name__}: SQL statement: {query_schema}")
        df = self.raw_query(query_schema, jdbc_options)
        return df.schema

    @property
    @abstractmethod
    def url(self) -> str:
        """"""

    def get_compare_statement(self, comparator: Callable, arg1: Any, arg2: Any) -> str:
        template = self.compare_statements[comparator]
        return template.format(arg1, self.get_value_sql(arg2))

    def get_value_sql(self, value: Any) -> str:
        """
        Transform the value into an SQL Dialect-supported form.
        """

        if isinstance(value, datetime):
            return self._get_datetime_value_sql(value)
        elif isinstance(value, date):
            return self._get_date_value_sql(value)
        elif isinstance(value, (int, float, Decimal)):
            return str(value)

        return f"'{value}'"

    def _get_datetime_value_sql(self, value: datetime) -> str:
        """
        Transform the value into an SQL Dialect-supported datetime.
        """
        result = value.isoformat()
        return f"'{result}'"

    def _get_date_value_sql(self, value: date) -> str:
        """
        Transform the value into an SQL Dialect-supported date.
        """
        result = value.isoformat()
        return f"'{result}'"
