from abc import abstractmethod
from logging import getLogger
from typing import Optional, Dict, Any, ClassVar
from dataclasses import dataclass, field

from onetl.connection.connection_helpers import get_sql_query
from onetl.connection.db_connection import DBConnection
from onetl.connection.connection_helpers import execute_query_without_partitioning

log = getLogger(__name__)

# FORBIDDEN_JDBC_PROPERTIES - properties that are not allowed to be overridden
# in the constructors of the DWReader and DBWriter classes
FORBIDDEN_JDBC_PROPERTIES = {"user", "password", "url", "driver"}

# JDBC_PROPERTIES - these are the parameters that will go into the properties parameter
# in the spark method pyspark.sql.DataFrame.write.jdbc
JDBC_PROPERTIES = [
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


@dataclass(frozen=True)
class JDBCConnection(DBConnection):
    host: str
    user: str
    password: str = field(repr=False)
    port: Optional[int] = None
    # Database in rdbms, schema in DBReader.
    # Difference like https://www.educba.com/postgresql-database-vs-schema/
    database: str = "default"
    extra: Dict = field(default_factory=dict)
    driver: ClassVar[str] = ""
    package: ClassVar[str] = ""

    @property
    @abstractmethod
    def url(self) -> str:
        """"""

    def read_table(  # type: ignore
        self,
        table: str,
        columns: Optional[str],
        hint: Optional[str],
        where: Optional[str],
        jdbc_options: Dict,
        **kwargs: Any,
    ) -> "pyspark.sql.DataFrame":

        if not jdbc_options.get("fetchsize"):
            log.debug("<fetchsize> task parameter wasn't specified; the reading will be slowed down!")

        if jdbc_options.get("sessionInitStatement"):
            log.debug(f"Init SQL statement: {jdbc_options.get('sessionInitStatement')}")

        sql_text = get_sql_query(
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

    def save_df(  # type: ignore
        self,
        df: "pyspark.sql.DataFrame",
        table: str,
        jdbc_options: Dict,
        mode: Optional[str] = "append",
        **kwargs: Any,
    ) -> None:
        """
        Save the DataFrame into RDB.

        """

        options = jdbc_options.copy()
        options = self.jdbc_params_creator(jdbc_options=options)

        log_pass = "PASSWORD='*****'" if options["properties"].get("password") else "NO_PASSWORD"

        log.debug(f"USER='{self.user}' {log_pass} DRIVER={options['properties']['driver']}")
        log.debug(f"JDBC_URL='{self.url}'")

        # for convenience. parameters accepted by spark.write.jdbc method
        #   spark.read.jdbc(
        #     url, table, mode,
        #     properties:  { "user" : "SYSTEM", "password" : "mypassword", ... })

        df.write.mode(mode).jdbc(table=table, **options)

    def get_schema(  # type: ignore
        self,
        table: str,
        columns: str,
        jdbc_options: Dict,
        **kwargs: Any,
    ) -> "pyspark.sql.types.StructType":
        jdbc_options = self.jdbc_params_creator(jdbc_options=jdbc_options)

        query_schema = f"(SELECT {columns} FROM {table} WHERE 1 = 0) T"
        jdbc_options["table"] = query_schema
        jdbc_options["properties"]["fetchsize"] = "0"

        log.info(f"{self.__class__.__name__}: Fetching table {table} schema")
        log.info(f"{self.__class__.__name__}: SQL statement: {query_schema}")
        df = execute_query_without_partitioning(parameters=jdbc_options, spark=self.spark, sql=query_schema)
        return df.schema

    def jdbc_params_creator(
        self,
        jdbc_options: Dict,
    ) -> Dict:
        options = jdbc_options.copy()

        # Method intended to check the passed parameter jdbc_options.
        # The check is needed so that the parameters defined in the DBCOnnection class are not
        # overridden in the DBReader and DBWriter classes
        if set(jdbc_options).intersection(FORBIDDEN_JDBC_PROPERTIES):
            raise ValueError(
                f"An invalid parameter "
                f"<{set(jdbc_options).intersection(FORBIDDEN_JDBC_PROPERTIES)}> "
                f"was entered."
                f"{FORBIDDEN_JDBC_PROPERTIES} parameters can be defined only in DBConnection object",
            )

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
            options[k] = str(v)
            if k in JDBC_PROPERTIES:
                properties[k] = str(v)

        for k in properties.keys():
            options.pop(k)

        options["properties"] = properties

        # Will add lower and upper bounds parameters if numPartitions parameter was set
        # If numPartitions was not set then jdbc_options will remain unchanged
        return options

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

        upper_bound = jdbc_options.get("upperBound")
        lower_bound = jdbc_options.get("lowerBound")

        if (not lower_bound or not upper_bound) and num_partitions:

            log.info("Getting <upperBound> and <lowerBound> options")

            query_upper_lower_bound = (
                f"(SELECT min({partition_column}) lower_bound,"
                f"max({partition_column}) upper_bound "
                f"FROM {table} ) T"
            )
            log.debug(f"SQL query\n{query_upper_lower_bound}")

            df_upper_lower_bound = execute_query_without_partitioning(
                parameters=jdbc_options,
                spark=self.spark,
                sql=query_upper_lower_bound,
            )

            # The sessionInitStatement parameter is removed because it only needs to be applied once.
            jdbc_options["properties"].pop("sessionInitStatement", None)

            tuple_upper_lower_bound = df_upper_lower_bound.collect()[0]

            if not lower_bound:
                jdbc_options["lowerBound"] = tuple_upper_lower_bound.lower_bound
                log.warning(
                    "<lowerBound> task parameter wasn't specified:"
                    f" auto generated value is <{tuple_upper_lower_bound.lower_bound}>",
                )

            if not upper_bound:
                jdbc_options["upperBound"] = tuple_upper_lower_bound.upper_bound
                log.warning(
                    f"<upperBound> task parameter wasn't specified:"
                    f" auto generated value is <{tuple_upper_lower_bound.upper_bound}>",
                )
        return jdbc_options
