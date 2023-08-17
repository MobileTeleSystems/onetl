#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import logging
import os
import textwrap
import warnings
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from etl_entities.instance import Host
from pydantic import Field, root_validator, validator

from onetl._internal import get_sql_query
from onetl._util.classproperty import classproperty
from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.spark import get_executor_total_cores, get_spark_version
from onetl._util.version import Version
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.dialect_mixins import (
    SupportColumnsList,
    SupportDfSchemaNone,
    SupportHintNone,
    SupportHWMColumnStr,
    SupportHWMExpressionStr,
    SupportTableWithDBSchema,
    SupportWhereStr,
)
from onetl.connection.db_connection.jdbc_mixin import JDBCMixin
from onetl.exception import MISSING_JVM_CLASS_MSG, TooManyParallelJobsError
from onetl.hooks import slot, support_hooks
from onetl.hwm import Statement
from onetl.impl import GenericOptions
from onetl.log import log_lines, log_with_indent

# do not import PySpark here, as we allow user to use `Greenplum.get_packages()` for creating Spark session
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


class GreenplumTableExistBehavior(str, Enum):
    APPEND = "append"
    REPLACE_ENTIRE_TABLE = "replace_entire_table"

    def __str__(self) -> str:
        return str(self.value)

    @classmethod  # noqa: WPS120
    def _missing_(cls, value: object):  # noqa: WPS120
        if str(value) == "overwrite":
            warnings.warn(
                "Mode `overwrite` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `replace_entire_table` instead",
                category=UserWarning,
                stacklevel=4,
            )
            return cls.REPLACE_ENTIRE_TABLE


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


@support_hooks
class Greenplum(JDBCMixin, DBConnection):
    """Greenplum connection. |support_hooks|

    Based on package ``io.pivotal:greenplum-spark:2.1.4``
    (`Pivotal connector for Spark <https://network.tanzu.vmware.com/products/vmware-greenplum#/releases/1287433/file_groups/13260>`_).

    .. warning::

        Before using this connector please take into account :ref:`greenplum-prerequisites`

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
        from pyspark.sql import SparkSession

        # Please ask your DevOps and Greenplum admin what port range
        # on Spark side can be used to accept requests from Greenplum segments

        extra = {
            "server.port": "49152-65535",
        }

        # Create Spark session with Greenplum connector loaded
        # See Prerequisites page for more details
        maven_packages = Greenplum.get_packages(spark_version="3.2")
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .config("spark.dynamicAllocation.maxExecutors", 10)
            .config("spark.executor.cores", 1)
            .getOrCreate()
        )

        # IMPORTANT!!!
        # Each job on the Spark executor make its own connection to Greenplum master node,
        # so we need to limit number of connections to avoid opening too many of them.
        #
        # Table size ~20Gb requires about 10 executors * cores,
        # ~50Gb requires ~ 20 executors * cores,
        # 100Gb+ requires 30 executors * cores.
        #
        # Cores number can be increased, but executors count should be reduced
        # to keep the same number of executors * cores.

        # Create connection
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

    class ReadOptions(JDBCMixin.JDBCOptions):
        """Pivotal's Greenplum Spark connector reading options.

        .. note ::

            You can pass any value
            `supported by connector <https://docs.vmware.com/en/VMware-Tanzu-Greenplum-Connector-for-Apache-Spark/2.1/tanzu-greenplum-connector-spark/GUID-read_from_gpdb.html>`_,
            even if it is not mentioned in this documentation.

            The set of supported options depends on connector version. See link above.

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

    class WriteOptions(JDBCMixin.JDBCOptions):
        """Pivotal's Greenplum Spark connector writing options.

        .. note ::

            You can pass any value
            `supported by connector <https://docs.vmware.com/en/VMware-Tanzu-Greenplum-Connector-for-Apache-Spark/2.1/tanzu-greenplum-connector-spark/GUID-write_to_gpdb.html>`_,
            even if it is not mentioned in this documentation.

            The set of supported options depends on connector version. See link above.

        .. warning::

            Some options, like ``url``, ``dbtable``, ``server.*``, ``pool.*``,
            etc are populated from connection attributes, and cannot be set in ``WriteOptions`` class

        Examples
        --------

        Write options initialization

        .. code:: python

            options = Greenplum.WriteOptions(
                if_exists="append",
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

        if_exists: GreenplumTableExistBehavior = Field(default=GreenplumTableExistBehavior.APPEND, alias="mode")
        """Behavior of writing data into existing table.

        Possible values:
            * ``append`` (default)
                Adds new rows into existing table.

                .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created using options provided by user
                    (``distributedBy`` and others).

                * Table exists
                    Data is appended to a table. Table has the same DDL as before writing data.

                    .. warning::

                        This mode does not check whether table already contains
                        rows from dataframe, so duplicated rows can be created.

                        Also Spark does not support passing custom options to
                        insert statement, like ``ON CONFLICT``, so don't try to
                        implement deduplication using unique indexes or constraints.

                        Instead, write to staging table and perform deduplication
                        using :obj:`~execute` method.

            * ``replace_entire_table``
                **Table is dropped and then created**.

                .. dropdown:: Behavior in details

                * Table does not exist
                    Table is created using options provided by user
                    (``distributedBy`` and others).

                * Table exists
                    Table content is replaced with dataframe content.

                    After writing completed, target table could either have the same DDL as
                    before writing data (``truncate=True``), or can be recreated (``truncate=False``).

        .. note::

            ``error`` and ``ignore`` modes are not supported.
        """

        @root_validator(pre=True)
        def mode_is_deprecated(cls, values):
            if "mode" in values:
                warnings.warn(
                    "Option `Greenplum.WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                    "Use `Greenplum.WriteOptions(if_exists=...)` instead",
                    category=UserWarning,
                    stacklevel=3,
                )
            return values

    class Dialect(  # noqa: WPS215
        SupportTableWithDBSchema,
        SupportColumnsList,
        SupportDfSchemaNone,
        SupportWhereStr,
        SupportHintNone,
        SupportHWMExpressionStr,
        SupportHWMColumnStr,
        DBConnection.Dialect,
    ):
        @classmethod
        def _get_datetime_value_sql(cls, value: datetime) -> str:
            result = value.isoformat()
            return f"cast('{result}' as timestamp)"

        @classmethod
        def _get_date_value_sql(cls, value: date) -> str:
            result = value.isoformat()
            return f"cast('{result}' as date)"

    host: Host
    database: str
    port: int = 5432
    extra: Extra = Extra()

    DRIVER: ClassVar[str] = "org.postgresql.Driver"
    CONNECTIONS_WARNING_LIMIT: ClassVar[int] = 31
    CONNECTIONS_EXCEPTION_LIMIT: ClassVar[int] = 100

    @slot
    @classmethod
    def get_packages(
        cls,
        scala_version: str | None = None,
        spark_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        .. warning::

            You should pass at least one parameter.

        Parameters
        ----------
        scala_version : str, optional
            Scala version in format ``major.minor``.

            If ``None``, ``spark_version`` is used to determine Scala version.

        spark_version : str, optional
            Spark version in format ``major.minor``.

            Used only if ``scala_version=None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import Greenplum

            Greenplum.get_packages(scala_version="2.11")
            Greenplum.get_packages(spark_version="3.2")

        """

        # Connector version is fixed, so we can perform checks for Scala/Spark version
        if scala_version:
            scala_ver = Version.parse(scala_version)
        elif spark_version:
            spark_ver = Version.parse(spark_version)
            if spark_ver.digits(2) > (3, 2) or spark_ver.digits(2) < (2, 3):
                raise ValueError(f"Spark version must be 2.3.x - 3.2.x, got {spark_ver}")
            scala_ver = get_default_scala_version(spark_ver)
        else:
            raise ValueError("You should pass either `scala_version` or `spark_version`")

        if scala_ver.digits(2) < (2, 11) or scala_ver.digits(2) > (2, 12):
            raise ValueError(f"Scala version must be 2.11 - 2.12, got {scala_ver}")

        return [f"io.pivotal:greenplum-spark_{scala_ver.digits(2)}:2.1.4"]

    @classproperty
    def package_spark_2_3(cls) -> str:
        """Get package name to be downloaded by Spark 2.3."""
        msg = "`Greenplum.package_2_3` will be removed in 1.0.0, use `Greenplum.get_packages(spark_version='2.3')` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "io.pivotal:greenplum-spark_2.11:2.1.4"

    @classproperty
    def package_spark_2_4(cls) -> str:
        """Get package name to be downloaded by Spark 2.4."""
        msg = "`Greenplum.package_2_4` will be removed in 1.0.0, use `Greenplum.get_packages(spark_version='2.4')` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "io.pivotal:greenplum-spark_2.11:2.1.4"

    @classproperty
    def package_spark_3_2(cls) -> str:
        """Get package name to be downloaded by Spark 3.2."""
        msg = "`Greenplum.package_3_2` will be removed in 1.0.0, use `Greenplum.get_packages(spark_version='3.2')` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "io.pivotal:greenplum-spark_2.12:2.1.4"

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

        parameters = "&".join(f"{k}={v}" for k, v in sorted(extra.items()))
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}?{parameters}".rstrip("?")

    @slot
    def read_source_as_df(
        self,
        source: str,
        columns: list[str] | None = None,
        hint: str | None = None,
        where: str | None = None,
        df_schema: StructType | None = None,
        start_from: Statement | None = None,
        end_at: Statement | None = None,
        options: ReadOptions | dict | None = None,
    ) -> DataFrame:
        read_options = self.ReadOptions.parse(options).dict(by_alias=True, exclude_none=True)
        log.info("|%s| Executing SQL query (on executor):", self.__class__.__name__)
        where = self.Dialect._condition_assembler(condition=where, start_from=start_from, end_at=end_at)
        query = get_sql_query(table=source, columns=columns, where=where)
        log_lines(log, query)

        df = self.spark.read.format("greenplum").options(**self._connector_params(source), **read_options).load()
        self._check_expected_jobs_number(df, action="read")

        if where:
            df = df.filter(where)

        if columns:
            df = df.selectExpr(*columns)

        log.info("|Spark| DataFrame successfully created from SQL statement ")
        return df

    @slot
    def write_df_to_target(
        self,
        df: DataFrame,
        target: str,
        options: WriteOptions | dict | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)
        options_dict = write_options.dict(by_alias=True, exclude_none=True, exclude={"if_exists"})

        self._check_expected_jobs_number(df, action="write")

        log.info("|%s| Saving data to a table %r", self.__class__.__name__, target)
        mode = "overwrite" if write_options.if_exists == GreenplumTableExistBehavior.REPLACE_ENTIRE_TABLE else "append"
        df.write.format("greenplum").options(
            **self._connector_params(target),
            **options_dict,
        ).mode(mode).save()

        log.info("|%s| Table %r is successfully written", self.__class__.__name__, target)

    @slot
    def get_df_schema(
        self,
        source: str,
        columns: list[str] | None = None,
        options: JDBCMixin.JDBCOptions | dict | None = None,
    ) -> StructType:
        log.info("|%s| Fetching schema of table %r", self.__class__.__name__, source)

        query = get_sql_query(source, columns=columns, where="1=0", compact=True)
        jdbc_options = self.JDBCOptions.parse(options).copy(update={"fetchsize": 0})

        log.debug("|%s| Executing SQL query (on driver):", self.__class__.__name__)
        log_lines(log, query, level=logging.DEBUG)

        df = self._query_on_driver(query, jdbc_options)
        log.info("|%s| Schema fetched", self.__class__.__name__)

        return df.schema

    @slot
    def get_min_max_bounds(
        self,
        source: str,
        column: str,
        expression: str | None = None,
        hint: str | None = None,
        where: str | None = None,
        options: JDBCMixin.JDBCOptions | dict | None = None,
    ) -> tuple[Any, Any]:
        log.info("|Spark| Getting min and max values for column %r", column)

        jdbc_options = self.JDBCOptions.parse(options).copy(update={"fetchsize": 1})

        query = get_sql_query(
            table=source,
            columns=[
                self.Dialect._expression_with_alias(
                    self.Dialect._get_min_value_sql(expression or column),
                    "min",
                ),
                self.Dialect._expression_with_alias(
                    self.Dialect._get_max_value_sql(expression or column),
                    "max",
                ),
            ],
            where=where,
        )

        log.info("|%s| Executing SQL query (on driver):", self.__class__.__name__)
        log_lines(log, query)

        df = self._query_on_driver(query, jdbc_options)
        row = df.collect()[0]
        min_value = row["min"]
        max_value = row["max"]

        log.info("|Spark| Received values:")
        log_with_indent(log, "MIN(%r) = %r", column, min_value)
        log_with_indent(log, "MAX(%r) = %r", column, max_value)

        return min_value, max_value

    @validator("spark")
    def _check_java_class_imported(cls, spark):
        java_class = "io.pivotal.greenplum.spark.GreenplumRelationProvider"

        try:
            try_import_java_class(spark, java_class)
        except Exception as e:
            spark_version = get_spark_version(spark).digits(2)
            msg = MISSING_JVM_CLASS_MSG.format(
                java_class=java_class,
                package_source=cls.__name__,
                args=f"spark_version='{spark_version}'",
            )
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Missing Java class", exc_info=e, stack_info=True)
            raise ValueError(msg) from e
        return spark

    def _connector_params(
        self,
        table: str,
    ) -> dict:
        schema, table_name = table.split(".")  # noqa: WPS414
        extra = self.extra.dict(by_alias=True, exclude_none=True)
        extra = {key: value for key, value in extra.items() if key.startswith("server.") or key.startswith("pool.")}
        return {
            "driver": self.DRIVER,
            "url": self.jdbc_url,
            "user": self.user,
            "password": self.password.get_secret_value(),
            "dbschema": schema,
            "dbtable": table_name,
            **extra,
        }

    def _options_to_connection_properties(self, options: JDBCMixin.JDBCOptions):
        # See https://github.com/pgjdbc/pgjdbc/pull/1252
        # Since 42.2.9 Postgres JDBC Driver added new option readOnlyMode=transaction
        # Which is not a desired behavior, because `.fetch()` method should always be read-only

        if not getattr(options, "readOnlyMode", None):
            options = options.copy(update={"readOnlyMode": "always"})

        return super()._options_to_connection_properties(options)

    def _get_server_setting(self, name: str) -> Any:
        query = f"""
                SELECT setting
                FROM   pg_settings
                WHERE  name = '{name}'
                """
        log.debug("|%s| Executing SQL query (on driver):")
        log_lines(log, query, level=logging.DEBUG)

        df = self._query_on_driver(query, self.JDBCOptions())
        result = df.collect()

        log.debug(
            "|%s| Query succeeded, resulting in-memory dataframe contains %d rows",
            len(result),
        )
        if result:
            return result[0][0]

        return None

    def _get_occupied_connections_count(self) -> int:
        # https://stackoverflow.com/a/5270806
        query = """
                SELECT SUM(numbackends)
                FROM pg_stat_database
                """
        log.debug("|%s| Executing SQL query (on driver):")
        log_lines(log, query, level=logging.DEBUG)

        df = self._query_on_driver(query, self.JDBCOptions())
        result = df.collect()

        log.debug(
            "|%s| Query succeeded, resulting in-memory dataframe contains %d rows",
            len(result),
        )
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

        expected_cores, config = get_executor_total_cores(self.spark)
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

        log_lines(log, message, level=logging.WARNING)

    def _log_parameters(self):
        super()._log_parameters()
        log_with_indent(log, "jdbc_url = %r", self.jdbc_url)
