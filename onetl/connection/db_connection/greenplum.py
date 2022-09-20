from __future__ import annotations

import logging
import os
import textwrap
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from pydantic import Field

from onetl._internal import (  # noqa: WPS436
    get_sql_query,
    spark_max_cores_with_config,
    suppress_logging,
    to_camel,
)
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.jdbc_mixin import JDBCMixin
from onetl.exception import TooManyParallelJobsError
from onetl.impl import GenericOptions
from onetl.log import log_with_indent, onetl_log

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = logging.getLogger(__name__)

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


@dataclass
class ConnectionLimits:
    maximum: int
    reserved: int
    occupied: int

    @property
    def available(self) -> int:
        return self.maximum - self.reserved - self.occupied

    @property
    def summary(self) -> str:
        return textwrap.dedent(
            f"""
            available connections: {self.available}
            occupied: {self.occupied}
            max: {self.maximum} ("max_connection" in postgresql.conf)
            reserved: {self.reserved} ("superuser_reserved_connections" in postgresql.conf)
            """,
        ).strip()


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
        User, which have proper access to the database. For example: ``some_user``

    password : str
        Password for database connection

    database : str
        Database in RDBMS, NOT schema.

        See `this page <https://www.educba.com/postgresql-database-vs-schema/>`_ for more details

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session.

        You can use ``mtspark`` for spark session initialization

    extra : dict, default: ``None``
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"tcpKeepAlive": "true", "server.port": "50000-65535"}``

        Supported options are:
            * All `Postgres JDBC driver properties <https://github.com/pgjdbc/pgjdbc#connection-properties>`_
            * Properties from `Greenplum connector for Spark documentation <https://docs.vmware.com/en/VMware-Tanzu-Greenplum-Connector-for-Apache-Spark/2.1/tanzu-greenplum-connector-spark/GUID-options.html>`_ page, but only starting with ``server.`` or ``pool.``

    Examples
    --------

    Greenplum connection initialization

    .. code:: python

        from onetl.connection import Greenplum
        from mtspark import get_spark

        extra = {
            "server.port": "49152-65535",
            "pool.maxSize": "10",
        }

        spark = get_spark(
            {
                "appName": "spark-app-name",
                "spark.jars.packages": [
                    "default:skip",
                    Greenplum.package_spark_3_2,  # package should match your Spark version
                ],
                # IMPORTANT!!!
                # Each job on the executor make its own connection to Spark,
                # so we need to limit them to avoid opening too many connections.
                # table size ~20Gb requires about 10 executors * cores,
                # ~50Gb requires ~ 20 executors * cores,
                # 100Gb+ requires 30 executors * cores.
                # Cores number can be increased, but executors count should be reduced
                # to keep the same number of executors * cores.
                #
                "spark.dynamicAllocation.maxExecutors": 10,  # or spark.executor.instances
                "spark.executor.cores": 1,
            },
        )

        greenplum = Greenplum(
            host="master.host.or.ip",
            user="user",
            password="*****",
            database="target_database",
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

            Greenplum.ReadOptions(
                partition_column="reg_id",
                num_partitions=10,
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
        """Column used to parallelize reading from a table.

        .. warning::

            You should not change this option, unless you know what you're doing

        Possible values:
            * ``None`` (default):
                Spark generates N jobs (where N == number of segments in Greenplum cluster),
                each job is reading only data from a specific segment
                (filtering data by ``gp_segment_id`` column).

                This is very effective way to fetch the data from a cluster.

            * table column
                Allocate each executor a range of values from a specific column.

                .. note::
                    Column type must be numeric. Other types are not supported.

                Spark generates for each executor an SQL query like:

                Executor 1:

                .. code:: sql

                    SELECT ... FROM table
                    WHERE (partition_column >= lowerBound
                            OR partition_column IS NULL)
                    AND partition_column < (lower_bound + stride)

                Executor 2:

                .. code:: sql

                    SELECT ... FROM table
                    WHERE partition_column >= (lower_bound + stride)
                    AND partition_column < (lower_bound + 2 * stride)

                ...

                Executor N:

                .. code:: sql

                    SELECT ... FROM table
                    WHERE partition_column >= (lower_bound + (N-1) * stride)
                    AND partition_column <= upper_bound

                Where ``stride=(upper_bound - lower_bound) / num_partitions``,
                ``lower_bound=MIN(partition_column)``, ``upper_bound=MAX(partition_column)``.

                .. note::

                    :obj:`~num_partitions` is used just to
                    calculate the partition stride, **NOT** for filtering the rows in table.
                    So all rows in the table will be returned (unlike *Incremental* :ref:`strategy`).

                .. note::

                    All queries are executed in parallel. To execute them sequentially, use *Batch* :ref:`strategy`.

        .. warning::

            Both options :obj:`~partition_column` and :obj:`~num_partitions` should have a value,
            or both should be ``None``

        Examples
        --------

        Read data in 10 parallel jobs by range of values in ``id_column`` column:

        .. code:: python

            Greenplum.ReadOptions(
                partition_column="id_column",
                num_partitions=10,
            )
        """

        num_partitions: Optional[int] = Field(alias="partitions")
        """Number of jobs created by Spark to read the table content in parallel.

        See documentation for :obj:`~partition_column` for more details

        .. warning::

            By default connector uses number of segments in the Greenplum cluster.
            You should not change this option, unless you know what you're doing

        .. warning::

            Both options :obj:`~partition_column` and :obj:`~num_partitions` should have a value,
            or both should be ``None``
        """

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

    CONNECTIONS_WARNING_LIMIT: ClassVar[int] = 31
    CONNECTIONS_EXCEPTION_LIMIT: ClassVar[int] = 100

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
        self._check_expected_jobs_number(df, action="read")

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

        self._check_expected_jobs_number(df, action="write")

        log.info(f"|{self.__class__.__name__}| Saving data to a table {table!r}")
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

    def _get_server_setting(self, name: str) -> Any:
        with suppress_logging(onetl_log):
            df = self.fetch(
                f"""
                SELECT setting
                FROM   pg_settings
                WHERE  name = '{name}'
                """,
            )

        result = df.collect()
        if result:
            return result[0][0]

        return None

    def _get_occupied_connections_count(self) -> int:
        # https://stackoverflow.com/a/5270806
        with suppress_logging(onetl_log):
            df = self.fetch(
                """
                SELECT SUM(numbackends)
                FROM pg_stat_database
                """,
            )

        result = df.collect()
        return int(result[0][0])

    def _get_connections_limits(self) -> ConnectionLimits:
        max_connections = int(self._get_server_setting("max_connections"))
        reserved_connections = int(self._get_server_setting("superuser_reserved_connections"))
        occupied_connections = self._get_occupied_connections_count()
        return ConnectionLimits(
            maximum=max_connections,
            reserved=reserved_connections,
            occupied=occupied_connections,
        )

    def _check_expected_jobs_number(self, df: DataFrame, action: str) -> None:
        # Parallel reading or writing to Greenplum can open a lot of connections.
        # Connection number is limited on server side, so we should prevent creating too much of them because reaching
        # the limit may prevent all other users from connecting the cluster

        # We cannot use `ReadOptions.partitions` because its default value
        # is calculated dynamically, based on number of segments in the Greenplum instance.
        # Also number of partitions in a writing process is determined by dataframe, not connector options
        partitions = df.rdd.getNumPartitions()
        if partitions < self.CONNECTIONS_WARNING_LIMIT:
            return

        expected_cores, config = spark_max_cores_with_config(self.spark)
        if expected_cores < self.CONNECTIONS_WARNING_LIMIT:
            return

        # each partition goes to its own core.
        # if there are no enough cores, the excess of partitions will wait for a free core
        max_jobs = min(expected_cores, partitions)

        limits = self._get_connections_limits()
        connections_message = (
            textwrap.dedent(
                f"""
                Each parallel job of {max_jobs} opens a separate connection.
                This can lead to reaching out the connection limit and disrupting other users:
                """,
            ).strip()
            + os.linesep
            + textwrap.indent(limits.summary, " " * 4)
        )

        session_options_recommendation = "reduce number of resources used by your Spark session:"
        session_options_recommendation += os.linesep
        session_options_recommendation += os.linesep.join(" " * 4 + f"{key}: {value}" for key, value in config.items())
        session_options_recommendation += os.linesep

        dataframe_options_recommendation = f"reduce number of partitions ({partitions}) in the dataframe using:"
        dataframe_options_recommendation += os.linesep + " " * 4
        if action == "read":
            dataframe_options_recommendation += (
                f'{self.__class__.__name__}.ReadOptions(partitions=N, partitionColumn="...")'
            )
        else:
            dataframe_options_recommendation += "df.repartition(N)"
        dataframe_options_recommendation += os.linesep

        if expected_cores > partitions or action == "read":
            # default partitioning method is more effective than custom partitioning,
            # so it's better to change session options
            recommendations = "Please " + session_options_recommendation + "or " + dataframe_options_recommendation
        else:
            # 50 executors, 200 partitions means that partitions will be written sequentially.
            # Greenplum is not designed for that, it's better to repartition dataframe.
            # Yes, it will take more RAM on each executor
            recommendations = "Please " + dataframe_options_recommendation + "or " + session_options_recommendation

        message = (connections_message + os.linesep * 2 + recommendations).strip()

        if max_jobs >= self.CONNECTIONS_EXCEPTION_LIMIT:
            raise TooManyParallelJobsError(message)

        log_with_indent(f"|{self.__class__.__name__}| {message}", level=logging.WARNING)

    def _log_parameters(self):
        super()._log_parameters()
        log_with_indent(f"jdbc_url = {self.jdbc_url!r}")

    def _get_datetime_value_sql(self, value: datetime) -> str:
        result = value.isoformat()
        return f"cast('{result}' as timestamp)"

    def _get_date_value_sql(self, value: date) -> str:
        result = value.isoformat()
        return f"cast('{result}' as date)"