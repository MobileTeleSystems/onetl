from abc import abstractmethod
from logging import getLogger
from typing import ClassVar, Dict, List, Optional, Set
from dataclasses import dataclass, field

from pydantic import Field, validator

from onetl.connection.connection_helpers import get_sql_query, LOG_INDENT
from onetl.connection.db_connection.db_connection import DBConnection

log = getLogger(__name__)


@dataclass(frozen=True)
class JDBCConnection(DBConnection):
    host: str
    user: str
    password: str = field(repr=False)
    # Database in rdbms, schema in DBReader.
    # Difference like https://www.educba.com/postgresql-database-vs-schema/
    database: Optional[str] = None
    port: Optional[int] = None
    extra: Dict = field(default_factory=dict)
    driver: ClassVar[str] = ""
    package: ClassVar[str] = ""

    class Options(DBConnection.Options):  # noqa: WPS431
        fetchsize: int = 100000
        batchsize: Optional[int] = None
        isolation_level: Optional[str] = Field(alias="isolationLevel", default=None)
        session_init_statement: Optional[str] = Field(alias="sessionInitStatement", default=None)
        truncate: Optional[bool] = None
        create_table_options: Optional[str] = Field(alias="createTableOptions", default=None)
        create_table_column_types: Optional[str] = Field(alias="createTableColumnTypes", default=None)
        custom_schema: Optional[str] = Field(alias="customSchema", default=None)
        query_timeout: Optional[int] = Field(alias="queryTimeout", default=None)
        cascade_truncate: Optional[bool] = Field(alias="cascadeTruncate", default=None)
        push_down_predicate: Optional[str] = Field(alias="pushDownPredicate", default=None)

        # Options in DataFrameWriter.jdbc() method
        partition_column: Optional[str] = Field(alias="partitionColumn", default=None)
        lower_bound: Optional[int] = Field(alias="lowerBound", default=None)
        upper_bound: Optional[int] = Field(alias="upperBound", default=None)
        num_partitions: Optional[int] = Field(alias="numPartitions", default=None)

        @validator("num_partitions", pre=True)
        def num_partitions_only_set_with_partition_column(cls, value, values):  # noqa: N805
            partition_column = values.get("partition_column")

            if value and not partition_column:
                raise ValueError("Option `num_partitions` could be set only with `partitionColumn`")

            return value

    @property
    @abstractmethod
    def jdbc_url(self) -> str:
        """JDBC Connection URL"""

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}"

    def check(self):
        options = {"properties": {"user": self.user, "password": self.password, "driver": self.driver}}

        log.info(f"|{self.__class__.__name__}| Check connection availability...")

        log.info("|Spark| Using connection:")
        log.info(" " * LOG_INDENT + f"type = {self.__class__.__name__}")
        log.info(" " * LOG_INDENT + f"jdbc_url = {self.jdbc_url}")
        log.info(" " * LOG_INDENT + f"driver = {self.driver}")
        log.info(" " * LOG_INDENT + f"user = {self.user}")

        log.info(f"|{self.__class__.__name__}| Execute statement:")
        log.info(" " * LOG_INDENT + self.check_statement)

        try:
            self.spark.read.jdbc(table=f"({self.check_statement}) T", url=self.jdbc_url, **options).collect()
            log.info(f"|{self.__class__.__name__}| Connection is available.")
        except Exception as e:
            msg = f"Connection is unavailable:\n{e}"
            log.exception(f"|{self.__class__.__name__}| {msg}")
            raise RuntimeError(msg) from e

    def read_table(  # type: ignore
        self,
        table: str,
        columns: Optional[List[str]],
        hint: Optional[str],
        where: Optional[str],
        options: Options,
    ) -> "pyspark.sql.DataFrame":
        if options.session_init_statement:
            log.debug("Init SQL statement:")
            log.debug(" " * LOG_INDENT + options.session_init_statement)

        self._log_parameters()

        sql_text = get_sql_query(
            table=table,
            hint=hint,
            columns=columns,
            where=where,
        )

        read_options = self.set_lower_upper_bound(jdbc_options=options.copy(exclude={"mode"}), table=table)

        log.info(f"|{self.__class__.__name__}| SQL statement:")
        log.info(" " * LOG_INDENT + sql_text)

        # for convenience. parameters accepted by spark.read.jdbc method
        #  spark.read.jdbc(
        #    url, table, column, lowerBound, upperBound, numPartitions, predicates
        #    properties:  { "user" : "SYSTEM", "password" : "mypassword", ... })

        dict_read_options = self.jdbc_params_creator(jdbc_options=read_options)
        df = self.spark.read.jdbc(table=f"({sql_text}) T", **dict_read_options)
        log.info("|Spark| DataFrame successfully created from SQL statement")
        return df

    def save_df(  # type: ignore
        self,
        df: "pyspark.sql.DataFrame",
        table: str,
        options: Options,
    ) -> None:
        """
        Save the DataFrame into RDB.
        """

        self._log_parameters()

        # for convenience. parameters accepted by spark.write.jdbc method
        #   spark.read.jdbc(
        #     url, table, mode,
        #     properties:  { "user" : "SYSTEM", "password" : "mypassword", ... })

        jdbc_options = self.jdbc_params_creator(jdbc_options=options)
        df.write.jdbc(table=table, **jdbc_options)
        log.info(f"|{self.__class__.__name__}| Table {table} successfully written")

    def get_schema(  # type: ignore
        self,
        table: str,
        columns: Optional[List[str]],
        options: Options,
    ) -> "pyspark.sql.types.StructType":

        query_schema = get_sql_query(table, columns=columns, where="1=0")
        temp_prop = options.copy(update={"fetchsize": "0"})
        log.info(f"|{self.__class__.__name__}| Fetching schema of {table}")
        log.info(f"|{self.__class__.__name__}| SQL statement:")
        log.info(" " * LOG_INDENT + query_schema)
        df = self.execute_query_without_partitioning(
            parameters=temp_prop,
            spark=self.spark,
            table=f"({query_schema}) T",
        )

        log.info(f"|{self.__class__.__name__}| Schema fetched")

        return df.schema

    def jdbc_params_creator(
        self,
        jdbc_options: Options,
    ) -> Dict:
        jdbc_options = jdbc_options.copy(
            update={
                "user": self.user,
                "password": self.password,
                "driver": self.driver,
                "url": self.jdbc_url,
            },
        )

        # Have to replace the <partitionColumn> parameter with <column>
        # since the method takes the named <column> parameter
        # link to source below
        # https://git.io/JKOku
        if jdbc_options.partition_column:  # noqa: WPS609
            jdbc_options = jdbc_options.copy(
                update={"column": jdbc_options.partition_column},
                exclude={"partition_column"},
            )

        top_level_options = {"url", "column", "lower_bound", "upper_bound", "num_partitions", "mode"}
        result = jdbc_options.dict(
            by_alias=True,
            include=top_level_options,
            exclude_none=True,
        )
        result["properties"] = jdbc_options.dict(
            by_alias=True,
            exclude=top_level_options,
            exclude_none=True,
        )

        def recursive_all_values_to_str(d: Dict):  # noqa: WPS430
            for k, v in d.items():
                if not isinstance(d[k], dict):
                    d[k] = str(v).lower() if str(v).lower() in {"true", "false"} else str(v)
                else:
                    recursive_all_values_to_str(d[k])
            return d

        return recursive_all_values_to_str(result)

    def set_lower_upper_bound(
        self,
        jdbc_options: Options,
        table: str,
    ) -> Options:
        """
        If numPatition was set then sets upperBound and lowerBound if not set ones.
        """

        # If the lowerBound and upperBound parameters are not set,
        # then the automatic calculation algorithm of the boundary is started.

        partition_column = jdbc_options.partition_column
        num_partitions = jdbc_options.num_partitions
        upper_bound = jdbc_options.upper_bound
        lower_bound = jdbc_options.lower_bound

        if (not lower_bound or not upper_bound) and num_partitions:

            log.info("|Spark| Getting <upperBound> and <lowerBound> options")

            query_upper_lower_bound = (
                f"(SELECT min({partition_column}) lower_bound,"
                f"max({partition_column}) upper_bound "
                f"FROM {table} ) T"
            )
            log.info(f"|{self.__class__.__name__}| SQL statement\n{query_upper_lower_bound}")

            df_upper_lower_bound = self.execute_query_without_partitioning(
                parameters=jdbc_options,
                spark=self.spark,
                table=query_upper_lower_bound,
            )

            # The sessionInitStatement parameter is removed because it only needs to be applied once.
            jdbc_options.__delattr__("session_init_statement")  # noqa: WPS609

            tuple_upper_lower_bound = df_upper_lower_bound.collect()[0]

            if not lower_bound:
                jdbc_options = jdbc_options.copy(update={"lower_bound": tuple_upper_lower_bound.lower_bound})
                log.warning(
                    "|Spark| <lowerBound> task parameter wasn't specified:"
                    f" auto generated value is <{tuple_upper_lower_bound.lower_bound}>",
                )

            if not upper_bound:
                jdbc_options = jdbc_options.copy(update={"upper_bound": tuple_upper_lower_bound.upper_bound})
                log.warning(
                    f"|Spark| <upperBound> task parameter wasn't specified:"
                    f" auto generated value is <{tuple_upper_lower_bound.upper_bound}>",
                )

        return jdbc_options

    def execute_query_without_partitioning(
        self,
        parameters: Options,
        spark: "pyspark.sql.SparkSession",
        table: str,
    ) -> "pyspark.sql.DataFrame":
        jdbc_dict_params = self.jdbc_params_creator(parameters)
        jdbc_dict_params.pop("numPartitions", None)
        jdbc_dict_params.pop("lowerBound", None)
        jdbc_dict_params.pop("upperBound", None)
        jdbc_dict_params.pop("partitionColumn", None)
        jdbc_dict_params.pop("column", None)
        jdbc_dict_params.pop("mode", None)

        return spark.read.jdbc(table=table, **jdbc_dict_params)

    @classmethod
    def _log_fields(cls) -> Set[str]:
        fields = super()._log_fields()
        fields.add("jdbc_url")
        return fields

    @classmethod
    def _log_exclude_fields(cls) -> Set[str]:
        fields = super()._log_exclude_fields()
        return fields.union({"password", "package"})
