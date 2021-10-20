from abc import abstractmethod
from dataclasses import dataclass, field
from logging import getLogger
from typing import Optional, Dict

from onetl.connection import ConnectionABC

log = getLogger(__name__)


@dataclass(frozen=True)
class DBConnection(ConnectionABC):
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

    jdbc_properties = [
        "user",
        "password",
        "driver",
        "fetchsize",
        "batchsize",
        "isolationLevel",
        "sessionInitStatement",
        "truncate",
        "createTableOptions",
        "createTableColumnTypes",
        "customSchema",
    ]

    def jdbc_params_creator(
        self,
        table: str,
        jdbc_options: Dict,
    ) -> Dict:
        options = jdbc_options.copy()

        options["user"] = self.user
        options["password"] = self.password
        options["driver"] = self.driver
        options["url"] = self.url

        # Have to replace the <partitionColumn> parameter with <column>
        # since the method takes the named <column> parameter
        # link to source below
        # https://git.io/JKOku
        if options.get("partitionColumn"):
            options["column"] = options["partitionColumn"]
            options.pop("partitionColumn")

        # Further below, the parameters are distributed to those that are inside the <properties> parameter
        # and those that are not included in it
        properties = {}
        for k, v in options.items():
            if k in self.jdbc_properties:
                properties[k] = v

        for k in properties.keys():
            options.pop(k)

        options["properties"] = properties

        # Will add lower and upper bounds parameters if numPartitions parameter was set
        # If numPartitions was not set then jdbc_options will remain unchanged
        return self.set_lower_upper_bound(jdbc_options=options, table=table)

    def read_table(
        self,
        jdbc_options: Dict,
        table: str,
        columns: Optional[str],
        sql_hint: Optional[str],
        sql_where: Optional[str],
    ) -> "pyspark.sql.DataFrame":

        # TODO:(@dypedchenk) Here will need to implement a hint in case of typos in writing parameters
        if not self.spark:
            raise ValueError("Spark session not provided")

        if not jdbc_options.get("fetchsize"):
            log.warning("<fetchsize> task parameter wasn't specified; the reading will be slowed down!")

        if jdbc_options.get("sessionInitStatement"):
            log.debug(f"Init SQL statement: {jdbc_options.get('sessionInitStatement')}")

        sql_text = self.get_sql_query(
            table=table,
            sql_hint=sql_hint,
            columns=columns,
            sql_where=sql_where,
        )

        log.debug(f"SQL statement: {sql_text}")

        options = self.jdbc_params_creator(
            jdbc_options=jdbc_options,
            table=sql_text,
        )

        log.debug(
            f"USER='{self.user}' " f"OPTIONS={options} DRIVER={options['properties']['driver']}",
        )
        log.debug(f"JDBC_URL='{self.url}'")
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
        options = self.jdbc_params_creator(jdbc_options=options, table=table)
        mode = options.pop("mode")
        format = options.pop("format")  # noqa: WPS125

        log_pass = "PASSWORD='*****'" if options["properties"].get("password") else "NO_PASSWORD"

        log.debug(f"USER='{self.user}' {log_pass} DRIVER={options['properties']['driver']}")
        log.debug(f"JDBC_URL='{self.url}'")
        df.write.mode(mode).format(format).jdbc(table=table, **options)

    def get_sql_query(
        self,
        table: str,
        sql_hint: Optional[str] = None,
        columns: Optional[str] = "*",
        sql_where: Optional[str] = None,
    ) -> str:
        """
        Creates a sql query for the dbtable parameter in the jdbc function.
        For instance: spark.read.jdbc(dbtable=sql)
        """
        sql_hint = f"/*+ {sql_hint} */" if sql_hint else None
        sql_where = f"WHERE {sql_where}" if sql_where else None

        statements = [
            "SELECT",
            sql_hint,
            columns,
            f"FROM {table}",
            sql_where,
        ]

        # The elements of the array with the value None are removed
        state: list = [x for x in statements if x]
        return " ".join(state)

    def set_lower_upper_bound(
        self,
        jdbc_options: Dict,
        table: str,
    ) -> Dict:
        """
        If numPatition was set then sets upperBound and lowerBound if not set ones.
        """

        # If the lowerBound and upperBound parameters are not set,
        # then the automatic calculation algorithm of the boundary is started.

        jdbc_options = jdbc_options.copy()

        partition_column = jdbc_options.get("column")
        num_partitions = jdbc_options.get("numPartitions")
        if num_partitions and not partition_column:
            raise ValueError("<partitionColumn> task parameter wasn't specified")

        if (not jdbc_options.get("lowerBound") or not jdbc_options.get("upperBound")) and num_partitions:

            if not self.spark:
                raise ValueError("Spark session not provided")

            log.info("Getting <upperBound> and <lowerBound> options")

            query_upper_lower_bound = (
                f"(SELECT min({partition_column}) lower_bound,"
                f"max({partition_column}) upper_bound "
                f"FROM {table} ) T"
            )
            log.debug(f"SQL query\n{query_upper_lower_bound}")

            jdbc_options_bounds_query = jdbc_options.copy()
            jdbc_options_bounds_query["table"] = query_upper_lower_bound
            jdbc_options_bounds_query.pop("numPartitions", None)
            jdbc_options_bounds_query.pop("lowerBound", None)
            jdbc_options_bounds_query.pop("upperBound", None)
            jdbc_options_bounds_query.pop("column", None)
            df_upper_lower_bound = self.spark.read.jdbc(**jdbc_options_bounds_query)

            # The sessionInitStatement parameter is removed because it only needs to be applied once.
            if jdbc_options["properties"].get("sessionInitStatement"):
                jdbc_options["properties"].pop("sessionInitStatement")

            tuple_upper_lower_bound = df_upper_lower_bound.collect()[0]

            if jdbc_options.get("lowerBound") is None:
                jdbc_options["lowerBound"] = tuple_upper_lower_bound.lower_bound
                log.warning(
                    "<lowerBound> task parameter wasn't specified:"
                    f" auto generated value is <{tuple_upper_lower_bound.lower_bound}>",
                )
            if jdbc_options.get("upperBound") is None:
                jdbc_options["upperBound"] = tuple_upper_lower_bound.upper_bound
                log.warning(
                    f"<upperBound> task parameter wasn't specified:"
                    f" auto generated value is <{tuple_upper_lower_bound.upper_bound}>",
                )
        return jdbc_options

    @property
    @abstractmethod
    def url(self) -> str:
        """"""

    def get_value_sql(self, value):
        """
        Transform the value into an SQL Dialect-supported form.

        :type value: HWM
        :rtype: str
        """
        if value.value_type in {"timestamp", "datetime"}:
            return self._get_timestamp_value_sql(value)
        return value.lit()

    @abstractmethod
    def _get_timestamp_value_sql(self, value):
        """
        Transform the value into an SQL Dialect-supported timestamp.

        :type value: Value
        :rtype: str
        """
