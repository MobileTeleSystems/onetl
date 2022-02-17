from abc import abstractmethod
from dataclasses import dataclass, field
from logging import getLogger
from typing import Any, ClassVar, Dict, List, Optional, Set, Tuple

from pydantic import Field, validator

from onetl.connection.db_connection.db_connection import DBConnection
from onetl.log import LOG_INDENT

log = getLogger(__name__)


@dataclass(frozen=True)
class JDBCConnection(DBConnection):
    host: str
    user: str
    password: str = field(repr=False)
    # Database in rdbms, schema in DBReader.
    # See https://www.educba.com/postgresql-database-vs-schema/ for more details
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
        self.log_parameters()

        log.info(f"|{self.__class__.__name__}| Checking connection availability...")
        log.info(f"|{self.__class__.__name__}| SQL statement:")
        log.info(" " * LOG_INDENT + self._check_query)

        options = {"properties": {"user": self.user, "password": self.password, "driver": self.driver}}
        try:
            self.spark.read.jdbc(table=f"({self._check_query}) T", url=self.jdbc_url, **options).collect()
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
        sql_text = self.get_sql_query_cte(
            table=table,
            where=where,
            cte_columns=columns,
            cte_hint=hint,
        )

        read_options = self.set_lower_upper_bound(
            table=table,
            columns=columns,
            where=where,
            hint=hint,
            options=options.copy(exclude={"mode"}),
        )

        log.info(f"|{self.__class__.__name__}| SQL statement:")
        log.info(" " * LOG_INDENT + sql_text)

        # for convenience. parameters accepted by spark.read.jdbc method
        #  spark.read.jdbc(
        #    url, table, column, lowerBound, upperBound, numPartitions, predicates
        #    properties:  { "user" : "SYSTEM", "password" : "mypassword", ... })

        dict_read_options = self.jdbc_params_creator(options=read_options)
        df = self.spark.read.jdbc(table=f"({sql_text}) T", **dict_read_options)
        log.info("|Spark| DataFrame successfully created from SQL statement")
        return df

    def save_df(  # type: ignore
        self,
        df: "pyspark.sql.DataFrame",
        table: str,
        options: Options,
    ) -> None:
        # for convenience. parameters accepted by spark.write.jdbc method
        #   spark.read.jdbc(
        #     url, table, mode,
        #     properties:  { "user" : "SYSTEM", "password" : "mypassword", ... })

        dict_write_options = self.jdbc_params_creator(options=options)
        df.write.jdbc(table=table, **dict_write_options)
        log.info(f"|{self.__class__.__name__}| Table {table} successfully written")

    def get_schema(  # type: ignore
        self,
        table: str,
        columns: Optional[List[str]],
        options: Options,
    ) -> "pyspark.sql.types.StructType":

        log.info(f"|{self.__class__.__name__}| Fetching schema of {table}")

        query_schema = self.get_sql_query(table, columns=columns, where="1=0")
        log.info(f"|{self.__class__.__name__}| SQL statement:")
        log.info(" " * LOG_INDENT + query_schema)

        temp_prop = options.copy(update={"fetchsize": "0"})
        df = self.execute_query_without_partitioning(
            parameters=temp_prop,
            spark=self.spark,
            table=f"({query_schema}) T",
        )

        log.info(f"|{self.__class__.__name__}| Schema fetched")

        return df.schema

    def jdbc_params_creator(
        self,
        options: Options,
    ) -> Dict:
        options = options.copy(
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
        if options.partition_column:  # noqa: WPS609
            options = options.copy(
                update={"column": options.partition_column},
                exclude={"partition_column"},
            )

        top_level_options = {"url", "column", "lower_bound", "upper_bound", "num_partitions", "mode"}
        result = options.dict(
            by_alias=True,
            include=top_level_options,
            exclude_none=True,
        )
        result["properties"] = options.dict(
            by_alias=True,
            exclude=top_level_options,
            exclude_none=True,
        )

        def recursive_all_values_to_str(d: dict):  # noqa: WPS430
            for k, v in d.items():
                if not isinstance(d[k], dict):
                    d[k] = str(v).lower() if str(v).lower() in {"true", "false"} else str(v)
                else:
                    recursive_all_values_to_str(d[k])
            return d

        return recursive_all_values_to_str(result)

    def get_min_max_bounds(  # type: ignore
        self,
        table: str,
        for_column: str,
        columns: Optional[List[str]],
        hint: Optional[str],
        where: Optional[str],
        options: Options,
    ) -> Tuple[Any, Any]:

        log.info(f'|Spark| Getting min and max values for column "{for_column}"')

        options = options.copy(update={"fetchsize": "1"})
        sql_text = self.get_sql_query_cte(
            table=table,
            columns=[
                self._get_min_value_sql(for_column, "min_value"),
                self._get_max_value_sql(for_column, "max_value"),
            ],
            where=where,
            cte_columns=columns,
            cte_hint=hint,
        )

        log.info(f"|{self.__class__.__name__}| SQL statement:")
        log.info(" " * LOG_INDENT + sql_text)

        df = self.execute_query_without_partitioning(
            parameters=options,
            spark=self.spark,
            table=f"({sql_text}) T",
        )

        row = df.collect()[0]
        min_value, max_value = row["min_value"], row["max_value"]

        log.info("|Spark| Received values:")
        log.info(" " * LOG_INDENT + f"MIN({for_column}) = {min_value}")
        log.info(" " * LOG_INDENT + f"MAX({for_column}) = {max_value}")

        return min_value, max_value

    def set_lower_upper_bound(  # type: ignore
        self,
        table: str,
        columns: Optional[List[str]],
        hint: Optional[str],
        where: Optional[str],
        options: Options,
    ) -> Options:
        """
        Determine values of upperBound and lowerBound options
        """

        if not options.partition_column:
            return options

        missing_values: List[str] = []

        if not options.lower_bound:
            missing_values.append("lowerBound")

        if not options.upper_bound:
            missing_values.append("upperBound")

        if not missing_values:
            return options

        log.warning(
            f"|Spark| numPartitions value is set to {options.num_partitions}, "
            f"but {' and '.join(missing_values)} value is not set. "
            f"It will be detected automatically based on values in partitionColumn {options.partition_column}",
        )

        min_partition_value, max_partition_value = self.get_min_max_bounds(
            table=table,
            for_column=options.partition_column,
            columns=columns,
            where=where,
            hint=hint,
            options=options,
        )

        # The sessionInitStatement parameter is removed because it only needs to be applied once.
        return options.copy(
            exclude={"session_init_statement"},
            update={
                "lower_bound": options.lower_bound or min_partition_value,
                "upper_bound": options.upper_bound or max_partition_value,
            },
        )

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
