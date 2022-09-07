from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from logging import getLogger
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from pydantic import Field

from onetl._internal import get_sql_query, to_camel  # noqa: WPS436
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.jdbc_mixin import JDBCMixin
from onetl.impl import GenericOptions
from onetl.log import log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = getLogger(__name__)

# options from which are populated by Greenplum class methods
GENERIC_PROHIBITED_OPTIONS = frozenset(
    (
        "dbschema",
        "dbtable",
    ),
)

EXTRA_OPTIONS = frozenset(
    (
        "server.*",
        "pool.*",
    ),
)

WRITE_OPTIONS = frozenset(
    (
        "mode",
        "truncate",
        "distributedBy",
        "distributed_by",
        "iteratorOptimization",
        "iterator_optimization",
    ),
)

READ_OPTIONS = frozenset(
    (
        "partitions",
        "num_partitions",
        "numPartitions",
        "partitionColumn",
        "partition_column",
    ),
)


class GreenplumWriteMode(str, Enum):  # noqa: WPS600
    APPEND = "append"
    OVERWRITE = "overwrite"

    def __str__(self) -> str:
        return str(self.value)


class Greenplum(JDBCMixin, DBConnection):
    """Class for Greenplum connection.

    Based on package ``io.pivotal:greenplum-spark:2.1.2``
    (`Pivotal connector for Spark <https://network.tanzu.vmware.com/products/vmware-tanzu-greenplum>`_)

    .. note::

        There is no public information which Greenplum server versions are compatible with this connector,
        please contact Pivotal

    Parameters
    ----------
    host : str
        Host of Greenplum master. For example: ``test.greenplum.domain.com`` or ``193.168.1.17``

    port : int, default: ``5432``
        Port of Greenplum master

    user : str
        User, which have access to the database and table. For example: ``some_user``

    password : str
        Password for database connection

    database : str
        Database in RDBMS, NOT schema.

        See `this page <https://www.educba.com/postgresql-database-vs-schema/>`_ for more details

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session that required for jdbc connection to database.

        You can use ``mtspark`` for spark session initialization.

    extra : dict, default: ``None``
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"tcpKeepAlive": "true", "server.port": "50000-65535"}``

        Supported options are:
            * All `Postgres JDBC driver properties <https://github.com/pgjdbc/pgjdbc#connection-properties>`_
            * Properties from `Greenplum connector for Spark documentation <https://docs.vmware.com/en/VMware-Tanzu-Greenplum-Connector-for-Apache-Spark/2.1/tanzu-greenplum-connector-spark/GUID-options.html>`_ page, but only starting with ``server.`` or ``pool.``

    Examples
    --------

    Greenplum connection initialization

    .. code::python

        from onetl.connection import Greenplum
        from mtspark import get_spark

        extra = {
            "server.port": "50000-65535",
            "server.timeout": "60000",
            "pool.hikari.maximumPoolSize": "40",
            "pool.hikari.poolName": "my-pool",
        }

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": [Greenplum.package_spark_3_2],  # package should match your Spark version
        })

        greenplum = Greenplum(
            host="master.host.or.ip",
            user="user",
            password="*****",
            database='target_database',
            extra=extra,
            spark=spark,
        )
    """

    class Extra(GenericOptions):
        # avoid closing connections from server side
        # while connector is moving data to executors before insert
        tcpKeepAlive: str = "true"  # noqa: N815

        class Config:
            extra = "allow"
            prohibited_options = JDBCMixin.JDBCOptions.Config.prohibited_options | GENERIC_PROHIBITED_OPTIONS

    class ReadOptions(JDBCMixin.JDBCOptions):  # noqa: WPS437
        """Class for Spark reading options, related to a specific JDBC source.

        .. note ::

            You can pass any value
            `supported by connector <https://docs.vmware.com/en/VMware-Tanzu-Greenplum-Connector-for-Apache-Spark/2.1/tanzu-greenplum-connector-spark/GUID-read_from_gpdb.html>`_,
            even if it is not mentioned in this documentation.

            The set of supported options depends on connector version.

        .. warning::

            Some options, like ``url``, ``dbtable``, ``server.*``, ``pool.*``,
            etc are populated from connection attributes, and cannot be set in ``ReadOptions`` class

        Examples
        --------

        Read options initialization

        .. code:: python

            options = Greenplum.ReadOptions(
                partitionColumn="reg_id",
                partitions=10,
            )
        """

        class Config:
            known_options = READ_OPTIONS
            prohibited_options = (
                JDBCMixin.JDBCOptions.Config.prohibited_options
                | EXTRA_OPTIONS
                | GENERIC_PROHIBITED_OPTIONS
                | WRITE_OPTIONS
            )
            alias_generator = to_camel

        partition_column: Optional[str] = Field(alias="partitionColumn")
        num_partitions: Optional[int] = Field(alias="partitions")

    class WriteOptions(JDBCMixin.JDBCOptions):  # noqa: WPS437
        """Class for writing options, related to Pivotal's Greenplum Spark connector

        .. note ::

            You can pass any value
            `supported by connector <https://docs.vmware.com/en/VMware-Tanzu-Greenplum-Connector-for-Apache-Spark/2.1/tanzu-greenplum-connector-spark/GUID-write_to_gpdb.html>`_,
            even if it is not mentioned in this documentation.

            The set of supported options depends on connector version.

        .. warning::

            Some options, like ``url``, ``dbtable``, ``server.*``, ``pool.*``,
            etc are populated from connection attributes, and cannot be set in ``WriteOptions`` class

        Examples
        --------

        Write options initialization

        .. code:: python

            options = Greenplum.WriteOptions(
                mode="append",
                truncate="false",
                distributedBy="mycolumn",
            )
        """

        class Config:
            known_options = WRITE_OPTIONS
            prohibited_options = (
                JDBCMixin.JDBCOptions.Config.prohibited_options
                | EXTRA_OPTIONS
                | GENERIC_PROHIBITED_OPTIONS
                | READ_OPTIONS
            )
            alias_generator = to_camel

        mode: GreenplumWriteMode = GreenplumWriteMode.APPEND
        """Mode of writing data into target table.

        Possible values:
            * ``append`` (default)
                Appends data into existing table.

                Behavior in details:

                * Table does not exist
                    Table is created using other options from current class
                    (``distributedBy`` and others).

                * Table exists
                    Data is appended to a table. Table has the same DDL as before writing data

            * ``overwrite``
                Overwrites data in the entire table (**table is dropped and then created, or truncated**).

                Behavior in details:

                * Table does not exist
                    Table is created using other options from current class
                    (``distributedBy`` and others).

                * Table exists
                    Table content is replaced with dataframe content.

                    After writing completed, target table could either have the same DDL as
                    before writing data (``truncate=True``), or can be recreated (``truncate=False``).

        .. note::

            ``error`` and ``ignore`` modes are not supported.
        """

    host: str
    database: str
    port: int = 5432
    extra: Extra = Extra()

    driver: ClassVar[str] = "org.postgresql.Driver"
    package_spark_2_3: ClassVar[str] = "io.pivotal:greenplum-spark_2.11:2.1.2"
    package_spark_2_4: ClassVar[str] = "io.pivotal:greenplum-spark_2.11:2.1.2"
    package_spark_3_2: ClassVar[str] = "io.pivotal:greenplum-spark_2.12:2.1.2"
    package_spark_3_3: ClassVar[str] = "io.pivotal:greenplum-spark_2.12:2.1.2"

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}/{self.database}"

    @property
    def jdbc_url(self) -> str:
        extra = {
            key: value
            for key, value in self.extra.dict(by_alias=True).items()
            if not (key.startswith("server.") or key.startswith("pool."))
        }
        extra["ApplicationName"] = extra.get("ApplicationName", self.spark.sparkContext.appName)

        params_str = "&".join(f"{k}={v}" for k, v in sorted(extra.items()))
        if params_str:
            params_str = f"?{params_str}"

        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}{params_str}"

    def read_table(
        self,
        table: str,
        columns: list[str] | None = None,
        hint: str | None = None,
        where: str | None = None,
        options: ReadOptions | dict | None = None,
    ) -> DataFrame:
        read_options = self.ReadOptions.parse(options).dict(by_alias=True, exclude_none=True)
        log.info(f"|{self.__class__.__name__}| Executing SQL query (on executor):")
        query = get_sql_query(table=table, columns=columns, hint=hint, where=where)
        log_with_indent(query)

        df = self.spark.read.format("greenplum").options(**self._connector_params(table), **read_options).load()

        if where:
            df = df.filter(where)

        if columns:
            df = df.selectExpr(*columns)

        if hint:
            df = df.hint(hint)

        log.info("|Spark| DataFrame successfully created from SQL statement ")

        return df

    def save_df(
        self,
        df: DataFrame,
        table: str,
        options: WriteOptions | dict | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)
        options_dict = write_options.dict(by_alias=True, exclude_none=True, exclude={"mode"})

        df.write.format("greenplum").options(
            **self._connector_params(table),
            **options_dict,
        ).mode(write_options.mode).save()
        log.info(f"|{self.__class__.__name__}| Table {table!r} successfully written")

    def get_schema(
        self,
        table: str,
        columns: list[str] | None = None,
        options: JDBCMixin.JDBCOptions | dict | None = None,
    ) -> StructType:

        log.info(f"|{self.__class__.__name__}| Fetching schema of table {table!r}")

        query = get_sql_query(table, columns=columns, where="1=0")
        jdbc_options = self.JDBCOptions.parse(options).copy(update={"fetchsize": 0})

        log.info(f"|{self.__class__.__name__}| Executing SQL query (on driver):")
        log_with_indent(query)

        df = self._query_on_driver(query, jdbc_options)
        log.info(f"|{self.__class__.__name__}| Schema fetched")

        return df.schema

    def get_min_max_bounds(
        self,
        table: str,
        column: str,
        expression: str | None = None,
        hint: str | None = None,
        where: str | None = None,
        options: JDBCMixin.JDBCOptions | dict | None = None,
    ) -> tuple[Any, Any]:
        log.info(f"|Spark| Getting min and max values for column {column!r}")

        jdbc_options = self.JDBCOptions.parse(options).copy(update={"fetchsize": 1})

        query = get_sql_query(
            table=table,
            columns=[
                self.expression_with_alias(self._get_min_value_sql(expression or column), f"min_{column}"),
                self.expression_with_alias(self._get_max_value_sql(expression or column), f"max_{column}"),
            ],
            where=where,
            hint=hint,
        )

        log.info(f"|{self.__class__.__name__}| Executing SQL query (on driver):")
        log_with_indent(query)

        df = self._query_on_driver(query, jdbc_options)

        min_value, max_value = df.collect()[0]
        log.info("|Spark| Received values:")
        log_with_indent(f"MIN({column}) = {min_value!r}")
        log_with_indent(f"MAX({column}) = {max_value!r}")

        return min_value, max_value

    def _connector_params(
        self,
        table: str,
    ) -> dict:
        schema, table_name = table.split(".")  # noqa: WPS414
        extra = self.extra.dict(by_alias=True, exclude_none=True)
        extra = {key: value for key, value in extra.items() if key.startswith("server.") or key.startswith("pool.")}
        return {
            "driver": self.driver,
            "url": self.jdbc_url,
            "user": self.user,
            "password": self.password.get_secret_value(),
            "dbschema": schema,
            "dbtable": table_name,
            **extra,
        }

    def _options_to_connection_properties(self, options: JDBCMixin.JDBCOptions):  # noqa: WPS437
        # See https://github.com/pgjdbc/pgjdbc/pull/1252
        # Since 42.2.9 Postgres JDBC Driver added new option readOnlyMode=transaction
        # Which is not a desired behavior, because `.fetch()` method should always be read-only

        if not getattr(options, "readOnlyMode", None):
            options = options.copy(update={"readOnlyMode": "always"})

        return super()._options_to_connection_properties(options)

    def _log_parameters(self):
        super()._log_parameters()
        log_with_indent(f"jdbc_url = {self.jdbc_url!r}")

    def _get_datetime_value_sql(self, value: datetime) -> str:
        result = value.isoformat()
        return f"cast('{result}' as timestamp)"

    def _get_date_value_sql(self, value: date) -> str:
        result = value.isoformat()
        return f"cast('{result}' as date)"
