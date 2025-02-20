# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import os
import textwrap
import warnings
from typing import TYPE_CHECKING, Any, ClassVar
from urllib.parse import quote, urlencode, urlparse, urlunparse

from etl_entities.instance import Host

from onetl.connection.db_connection.jdbc_connection.options import JDBCReadOptions

try:
    from pydantic.v1 import SecretStr, validator
except (ImportError, AttributeError):
    from pydantic import validator, SecretStr  # type: ignore[no-redef, assignment]

from onetl._util.classproperty import classproperty
from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.spark import (
    get_client_info,
    get_executor_total_cores,
    get_spark_version,
    override_job_description,
)
from onetl._util.version import Version
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.greenplum.connection_limit import (
    GreenplumConnectionLimit,
)
from onetl.connection.db_connection.greenplum.dialect import GreenplumDialect
from onetl.connection.db_connection.greenplum.options import (
    GreenplumExecuteOptions,
    GreenplumFetchOptions,
    GreenplumReadOptions,
    GreenplumSQLOptions,
    GreenplumTableExistBehavior,
    GreenplumWriteOptions,
)
from onetl.connection.db_connection.jdbc_mixin import JDBCMixin
from onetl.connection.db_connection.jdbc_mixin.options import (
    JDBCExecuteOptions,
    JDBCFetchOptions,
)
from onetl.connection.db_connection.jdbc_mixin.options import (
    JDBCOptions as JDBCMixinOptions,
)
from onetl.exception import MISSING_JVM_CLASS_MSG, TooManyParallelJobsError
from onetl.hooks import slot, support_hooks
from onetl.hwm import Window
from onetl.impl import GenericOptions
from onetl.log import log_lines, log_with_indent

# do not import PySpark here, as we allow user to use `Greenplum.get_packages()` for creating Spark session
if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = logging.getLogger(__name__)

EXTRA_OPTIONS = frozenset(
    (
        "server.*",
        "pool.*",
    ),
)


class GreenplumExtra(GenericOptions):
    # avoid closing connections from server side
    # while connector is moving data to executors before insert
    tcpKeepAlive: str = "true"  # noqa: N815

    class Config:
        extra = "allow"
        prohibited_options = JDBCMixinOptions.Config.prohibited_options


@support_hooks
class Greenplum(JDBCMixin, DBConnection):  # noqa: WPS338
    """Greenplum connection. |support_hooks|

    Based on package ``io.pivotal:greenplum-spark:2.2.0``
    (`VMware Greenplum connector for Spark <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/index.html>`_).

    .. seealso::

        Before using this connector please take into account :ref:`greenplum-prerequisites`

    .. versionadded:: 0.5.0

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
            * All `Postgres JDBC driver properties <https://jdbc.postgresql.org/documentation/use/>`_
            * Properties from `Greenplum connector for Spark documentation <https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/options.html>`_ page, but only starting with ``server.`` or ``pool.``

    Examples
    --------

    Create and check Greenplum connection:

    .. code:: python

        from onetl.connection import Greenplum
        from pyspark.sql import SparkSession

        # Create Spark session with Greenplum connector loaded
        maven_packages = Greenplum.get_packages(spark_version="3.2")
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .config("spark.executor.allowSparkContext", "true")
            # IMPORTANT!!!
            # Set number of executors according to "Prerequisites" -> "Number of executors"
            .config("spark.dynamicAllocation.maxExecutors", 10)
            .config("spark.executor.cores", 1)
            .getOrCreate()
        )

        # IMPORTANT!!!
        # Set port range of executors according to "Prerequisites" -> "Network ports"
        extra = {
            "server.port": "41000-42000",
        }

        # Create connection
        greenplum = Greenplum(
            host="master.host.or.ip",
            user="user",
            password="*****",
            database="target_database",
            extra=extra,
            spark=spark,
        ).check()
    """

    host: Host
    user: str
    password: SecretStr
    database: str
    port: int = 5432
    extra: GreenplumExtra = GreenplumExtra()

    ReadOptions = GreenplumReadOptions
    WriteOptions = GreenplumWriteOptions
    SQLOptions = GreenplumSQLOptions
    FetchOptions = GreenplumFetchOptions
    ExecuteOptions = GreenplumExecuteOptions
    JDBCOptions = JDBCMixinOptions

    Extra = GreenplumExtra
    Dialect = GreenplumDialect

    DRIVER: ClassVar[str] = "org.postgresql.Driver"
    CONNECTIONS_WARNING_LIMIT: ClassVar[int] = 31
    CONNECTIONS_EXCEPTION_LIMIT: ClassVar[int] = 100

    _CHECK_QUERY: ClassVar[str] = "SELECT 1"
    # any small table with always present in db, and which any user can access
    # https://www.greenplumdba.com/pg-catalog-tables-and-views
    _CHECK_DUMMY_TABLE: ClassVar[str] = "pg_catalog.gp_id"

    @slot
    @classmethod
    def get_packages(
        cls,
        *,
        scala_version: str | None = None,
        spark_version: str | None = None,
        package_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        .. warning::

            You should pass either ``scala_version`` or ``spark_version``.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        scala_version : str, optional
            Scala version in format ``major.minor``.

            If ``None``, ``spark_version`` is used to determine Scala version.

        spark_version : str, optional
            Spark version in format ``major.minor``.

            Used only if ``scala_version=None``.

        package_version : str, optional, default ``2.2.0``
            Package version in format ``major.minor.patch``

            .. versionadded:: 0.10.1

        Examples
        --------

        .. code:: python

            from onetl.connection import Greenplum

            Greenplum.get_packages(scala_version="2.12")
            Greenplum.get_packages(spark_version="3.2", package_version="2.3.0")

        """

        # Connector version is fixed, so we can perform checks for Scala/Spark version
        if package_version:
            package_ver = Version(package_version)
        else:
            package_ver = Version("2.2.0")

        if scala_version:
            scala_ver = Version(scala_version).min_digits(2)
        elif spark_version:
            spark_ver = Version(spark_version).min_digits(2)
            if spark_ver >= Version("3.3") or spark_ver < Version("2.3"):
                raise ValueError(f"Spark version must be 2.3.x - 3.2.x, got {spark_ver}")
            scala_ver = get_default_scala_version(spark_ver)
        else:
            raise ValueError("You should pass either `scala_version` or `spark_version`")

        if scala_ver < Version("2.11") or scala_ver > Version("2.12"):
            raise ValueError(f"Scala version must be 2.11 - 2.12, got {scala_ver.format('{0}.{1}')}")

        return [f"io.pivotal:greenplum-spark_{scala_ver.format('{0}.{1}')}:{package_ver}"]

    @classproperty
    def package_spark_2_3(cls) -> str:
        """Get package name to be downloaded by Spark 2.3."""
        msg = "`Greenplum.package_2_3` will be removed in 1.0.0, use `Greenplum.get_packages(spark_version='2.3')` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "io.pivotal:greenplum-spark_2.11:2.2.0"

    @classproperty
    def package_spark_2_4(cls) -> str:
        """Get package name to be downloaded by Spark 2.4."""
        msg = "`Greenplum.package_2_4` will be removed in 1.0.0, use `Greenplum.get_packages(spark_version='2.4')` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "io.pivotal:greenplum-spark_2.11:2.2.0"

    @classproperty
    def package_spark_3_2(cls) -> str:
        """Get package name to be downloaded by Spark 3.2."""
        msg = "`Greenplum.package_3_2` will be removed in 1.0.0, use `Greenplum.get_packages(spark_version='3.2')` instead"
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "io.pivotal:greenplum-spark_2.12:2.2.0"

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}/{self.database}"

    def __str__(self):
        return f"{self.__class__.__name__}[{self.host}:{self.port}/{self.database}]"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def jdbc_custom_params(self) -> dict:
        result = {
            key: value
            for key, value in self.extra.dict(by_alias=True).items()
            if not (key.startswith("server.") or key.startswith("pool."))
        }
        # https://www.postgresql.org/docs/current/runtime-config-logging.html#GUC-APPLICATION-NAME
        result["ApplicationName"] = result.get("ApplicationName", get_client_info(self.spark, limit=64))
        return result

    @property
    def jdbc_params(self) -> dict:
        result = super().jdbc_params
        result.update(self.jdbc_custom_params)
        return result

    @slot
    def check(self):
        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()  # type: ignore

        log.debug("|%s| Executing SQL query:", self.__class__.__name__)
        log_lines(log, self._CHECK_QUERY, level=logging.DEBUG)

        try:
            with override_job_description(self.spark, f"{self}.check()"):
                self._query_optional_on_driver(self._CHECK_QUERY, self.FetchOptions(fetchsize=1))

                read_options = self._get_connector_params(self._CHECK_DUMMY_TABLE)
                read_options["num_partitions"] = 1  # do not require gp_segment_id column in table
                df = self.spark.read.format("greenplum").options(**read_options).load()
                df.take(1)

            log.info("|%s| Connection is available.", self.__class__.__name__)
        except Exception as e:
            log.exception("|%s| Connection is unavailable", self.__class__.__name__)
            raise RuntimeError("Connection is unavailable") from e

        return self

    @slot
    def read_source_as_df(
        self,
        source: str,
        columns: list[str] | None = None,
        hint: str | None = None,
        where: str | None = None,
        df_schema: StructType | None = None,
        window: Window | None = None,
        limit: int | None = None,
        options: GreenplumReadOptions | None = None,
    ) -> DataFrame:
        read_options = self.ReadOptions.parse(options).dict(by_alias=True, exclude_none=True)
        log.info("|%s| Executing SQL query (on executor):", self.__class__.__name__)
        where = self.dialect.apply_window(where, window)
        fake_query_for_log = self.dialect.get_sql_query(table=source, columns=columns, where=where, limit=limit)
        log_lines(log, fake_query_for_log)

        df = self.spark.read.format("greenplum").options(**self._get_connector_params(source), **read_options).load()
        self._check_expected_jobs_number(df, action="read")

        if where:
            for item in where:
                df = df.filter(item)

        if columns:
            df = df.selectExpr(*columns)

        if limit is not None:
            df = df.limit(limit)

        log.info("|Spark| DataFrame successfully created from SQL statement ")
        return df

    @slot
    def write_df_to_target(
        self,
        df: DataFrame,
        target: str,
        options: GreenplumWriteOptions | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)
        options_dict = write_options.dict(by_alias=True, exclude_none=True, exclude={"if_exists"})

        self._check_expected_jobs_number(df, action="write")

        log.info("|%s| Saving data to a table %r", self.__class__.__name__, target)
        mode = (
            "overwrite"
            if write_options.if_exists == GreenplumTableExistBehavior.REPLACE_ENTIRE_TABLE
            else write_options.if_exists.value
        )
        df.write.format("greenplum").options(
            **self._get_connector_params(target),
            **options_dict,
        ).mode(mode).save()

        log.info("|%s| Table %r is successfully written", self.__class__.__name__, target)

    @slot
    def get_df_schema(
        self,
        source: str,
        columns: list[str] | None = None,
        options: JDBCReadOptions | None = None,
    ) -> StructType:
        log.info("|%s| Detected dialect: '%s'", self.__class__.__name__, self._get_spark_dialect_name())
        log.info("|%s| Fetching schema of table %r ...", self.__class__.__name__, source)

        query = self.dialect.get_sql_query(source, columns=columns, limit=0, compact=True)
        jdbc_options = self.ReadOptions.parse(options).copy(update={"fetchsize": 0})

        log.debug("|%s| Executing SQL query (on driver):", self.__class__.__name__)
        log_lines(log, query, level=logging.DEBUG)

        df = self._query_on_driver(query, self.FetchOptions.parse(jdbc_options.dict()))
        log.info("|%s| Schema fetched.", self.__class__.__name__)

        return df.schema

    @slot
    def get_min_max_values(
        self,
        source: str,
        window: Window,
        hint: Any | None = None,
        where: Any | None = None,
        options: JDBCReadOptions | None = None,
    ) -> tuple[Any, Any]:
        log.info("|%s| Getting min and max values for %r ...", self.__class__.__name__, window.expression)
        jdbc_options = self.ReadOptions.parse(options).copy(update={"fetchsize": 1})

        query = self.dialect.get_sql_query(
            table=source,
            columns=[
                self.dialect.aliased(
                    self.dialect.get_min_value(window.expression),
                    self.dialect.escape_column("min"),
                ),
                self.dialect.aliased(
                    self.dialect.get_max_value(window.expression),
                    self.dialect.escape_column("max"),
                ),
            ],
            where=self.dialect.apply_window(where, window),
        )

        log.info("|%s| Executing SQL query (on driver):", self.__class__.__name__)
        log_lines(log, query)

        df = self._query_on_driver(query, self.FetchOptions.parse(jdbc_options.dict()))
        row = df.collect()[0]
        min_value = row["min"]
        max_value = row["max"]

        log.info("|%s| Received values:", self.__class__.__name__)
        log_with_indent(log, "MIN(%s) = %r", window.expression, min_value)
        log_with_indent(log, "MAX(%s) = %r", window.expression, max_value)

        return min_value, max_value

    @validator("spark")
    def _check_java_class_imported(cls, spark):
        java_class = "io.pivotal.greenplum.spark.GreenplumRelationProvider"

        try:
            try_import_java_class(spark, java_class)
        except Exception as e:
            spark_version = get_spark_version(spark).format("{major}.{minor}")
            msg = MISSING_JVM_CLASS_MSG.format(
                java_class=java_class,
                package_source=cls.__name__,
                args=f"spark_version='{spark_version}'",
            )
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Missing Java class", exc_info=e, stack_info=True)
            raise ValueError(msg) from e
        return spark

    def _get_connector_params(
        self,
        table: str,
    ) -> dict:
        schema, table_name = table.split(".")  # noqa: WPS414
        extra = self.extra.dict(by_alias=True, exclude_none=True)
        greenplum_connector_options = {
            key: value for key, value in extra.items() if key.startswith("server.") or key.startswith("pool.")
        }

        # Greenplum connector requires all JDBC params to be passed via JDBC URL:
        # https://docs.vmware.com/en/VMware-Greenplum-Connector-for-Apache-Spark/2.3/greenplum-connector-spark/using_the_connector.html#specifying-session-parameters
        parsed_jdbc_url = urlparse(self.jdbc_url)
        sorted_jdbc_params = [(k, v) for k, v in sorted(self.jdbc_custom_params.items(), key=lambda x: x[0].lower())]
        jdbc_url_query = urlencode(sorted_jdbc_params, quote_via=quote)
        jdbc_url = urlunparse(parsed_jdbc_url._replace(query=jdbc_url_query))

        return {
            "driver": self.DRIVER,
            "url": jdbc_url,
            "user": self.user,
            "password": self.password.get_secret_value(),
            "dbschema": schema,
            "dbtable": table_name,
            **greenplum_connector_options,
        }

    def _get_jdbc_connection(self, options: JDBCFetchOptions | JDBCExecuteOptions, read_only: bool):
        if read_only:
            # To properly support pgbouncer, we have to create connection with readOnly option set.
            # See https://github.com/pgjdbc/pgjdbc/issues/848
            options = options.copy(update={"readOnly": True})

        connection_properties = self._options_to_connection_properties(options)
        driver_manager = self.spark._jvm.java.sql.DriverManager  # type: ignore
        # avoid calling .setReadOnly(True) here
        return driver_manager.getConnection(self.jdbc_url, connection_properties)

    def _get_server_setting(self, name: str) -> Any:
        query = f"""
                SELECT setting
                FROM   pg_settings
                WHERE  name = '{name}'
                """
        log.debug("|%s| Executing SQL query (on driver):")
        log_lines(log, query, level=logging.DEBUG)

        df = self._query_on_driver(query, self.FetchOptions())
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

        df = self._query_on_driver(query, self.FetchOptions())
        result = df.collect()

        log.debug(
            "|%s| Query succeeded, resulting in-memory dataframe contains %d rows",
            len(result),
        )
        return int(result[0][0])

    def _get_connections_limits(self) -> GreenplumConnectionLimit:
        max_connections = int(self._get_server_setting("max_connections"))
        reserved_connections = int(self._get_server_setting("superuser_reserved_connections"))
        occupied_connections = self._get_occupied_connections_count()
        return GreenplumConnectionLimit(
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
