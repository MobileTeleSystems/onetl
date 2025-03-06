# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from textwrap import dedent
from typing import TYPE_CHECKING, Any, ClassVar, Iterable

from etl_entities.instance import Cluster

try:
    from pydantic.v1 import validator
except (ImportError, AttributeError):
    from pydantic import validator  # type: ignore[no-redef, assignment]

from onetl._metrics.recorder import SparkMetricsRecorder
from onetl._util.spark import inject_spark_param, override_job_description
from onetl._util.sql import clear_statement
from onetl.base import BaseWritableFileFormat
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.hive.dialect import HiveDialect
from onetl.connection.db_connection.hive.options import (
    HiveLegacyOptions,
    HiveTableExistBehavior,
    HiveWriteOptions,
)
from onetl.connection.db_connection.hive.slots import HiveSlots
from onetl.file.format.file_format import ReadWriteFileFormat, WriteOnlyFileFormat
from onetl.hooks import slot, support_hooks
from onetl.hwm import Window
from onetl.log import log_lines, log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

PARTITION_OVERWRITE_MODE_PARAM = "spark.sql.sources.partitionOverwriteMode"
log = logging.getLogger(__name__)


@support_hooks
class Hive(DBConnection):
    """Spark connection with Hive MetaStore support. |support_hooks|

    .. seealso::

        Before using this connector please take into account :ref:`hive-prerequisites`

    .. versionadded:: 0.1.0

    Parameters
    ----------
    cluster : str
        Cluster name. Used for HWM and lineage.

        .. versionadded:: 0.7.0

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session with Hive metastore support enabled

    Examples
    --------

    .. tabs::

        .. tab:: Create Hive connection with Kerberos auth

            Execute ``kinit`` consome command before creating Spark Session

            .. code:: bash

                $ kinit -kt /path/to/keytab user

            .. code:: python

                from onetl.connection import Hive
                from pyspark.sql import SparkSession

                # Create Spark session
                # Use names "spark.yarn.access.hadoopFileSystems", "spark.yarn.principal"
                # and "spark.yarn.keytab" for Spark 2

                spark = (
                    SparkSession.builder.appName("spark-app-name")
                    .option("spark.kerberos.access.hadoopFileSystems", "hdfs://cluster.name.node:8020")
                    .option("spark.kerberos.principal", "user")
                    .option("spark.kerberos.keytab", "/path/to/keytab")
                    .enableHiveSupport()
                    .getOrCreate()
                )

                # Create connection
                hive = Hive(cluster="rnd-dwh", spark=spark).check()

        .. code-tab:: py Create Hive connection with anonymous auth

            from onetl.connection import Hive
            from pyspark.sql import SparkSession

            # Create Spark session
            spark = SparkSession.builder.appName("spark-app-name").enableHiveSupport().getOrCreate()

            # Create connection
            hive = Hive(cluster="rnd-dwh", spark=spark).check()
    """

    cluster: Cluster

    Dialect = HiveDialect
    WriteOptions = HiveWriteOptions
    Options = HiveLegacyOptions
    Slots = HiveSlots
    # TODO: remove in v1.0.0
    slots = HiveSlots

    _CHECK_QUERY: ClassVar[str] = "SHOW DATABASES"

    @slot
    @classmethod
    def get_current(cls, spark: SparkSession):
        """
        Create connection for current cluster. |support_hooks|

        .. note::

            Can be used only if there are some hooks bound to
            :obj:`Slots.get_current_cluster <onetl.connection.db_connection.hive.slots.HiveSlots.get_current_cluster>` slot.

        .. versionadded:: 0.7.0

        Parameters
        ----------
        spark : :obj:`pyspark.sql.SparkSession`
            Spark session

        Examples
        --------

        .. code:: python

            from onetl.connection import Hive
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.appName("spark-app-name").enableHiveSupport().getOrCreate()

            # injecting current cluster name via hooks mechanism
            hive = Hive.get_current(spark=spark)
        """

        log.info("|%s| Detecting current cluster...", cls.__name__)
        current_cluster = cls.Slots.get_current_cluster()
        if not current_cluster:
            raise RuntimeError(
                f"{cls.__name__}.get_current() can be used only if there are "
                f"some hooks bound to {cls.__name__}.Slots.get_current_cluster",
            )

        log.info("|%s| Got %r", cls.__name__, current_cluster)
        return cls(cluster=current_cluster, spark=spark)  # type: ignore[arg-type]

    @property
    def instance_url(self) -> str:
        return self.cluster

    def __str__(self):
        return f"{self.__class__.__name__}[{self.cluster}]"

    @slot
    def check(self):
        log.debug("|%s| Detecting current cluster...", self.__class__.__name__)
        current_cluster = self.Slots.get_current_cluster()
        if current_cluster and self.cluster != current_cluster:
            raise ValueError("You can connect to a Hive cluster only from the same cluster")

        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()

        log.debug("|%s| Executing SQL query:", self.__class__.__name__)
        log_lines(log, self._CHECK_QUERY, level=logging.DEBUG)

        try:
            with override_job_description(self.spark, f"{self}.check()"):
                self._execute_sql(self._CHECK_QUERY).take(1)
            log.info("|%s| Connection is available.", self.__class__.__name__)
        except Exception as e:
            log.exception("|%s| Connection is unavailable", self.__class__.__name__)
            raise RuntimeError("Connection is unavailable") from e

        return self

    @slot
    def sql(
        self,
        query: str,
    ) -> DataFrame:
        """
        Lazily execute SELECT statement and return DataFrame. |support_hooks|

        Same as ``spark.sql(query)``.

        .. versionadded:: 0.2.0

        Parameters
        ----------
        query : str

            SQL query to be executed.

        Returns
        -------
        df : pyspark.sql.dataframe.DataFrame

            Spark dataframe
        """

        query = clear_statement(query)

        log.info("|%s| Executing SQL query:", self.__class__.__name__)
        log_lines(log, query)

        with SparkMetricsRecorder(self.spark) as recorder:
            try:
                with override_job_description(self.spark, f"{self}.sql()"):
                    df = self._execute_sql(query)
            except Exception:
                log.error("|%s| Query failed", self.__class__.__name__)

                metrics = recorder.metrics()
                if log.isEnabledFor(logging.DEBUG) and not metrics.is_empty:
                    # as SparkListener results are not guaranteed to be received in time,
                    # some metrics may be missing. To avoid confusion, log only in debug, and with a notice
                    log.info("|%s| Recorded metrics (some values may be missing!):", self.__class__.__name__)
                    log_lines(log, str(metrics), level=logging.DEBUG)
                raise

            log.info("|Spark| DataFrame successfully created from SQL statement")

            metrics = recorder.metrics()
            if log.isEnabledFor(logging.DEBUG) and not metrics.is_empty:
                # as SparkListener results are not guaranteed to be received in time,
                # some metrics may be missing. To avoid confusion, log only in debug, and with a notice
                log.info("|%s| Recorded metrics (some values may be missing!):", self.__class__.__name__)
                log_lines(log, str(metrics), level=logging.DEBUG)

        return df

    @slot
    def execute(
        self,
        statement: str,
    ) -> None:
        """
        Execute DDL or DML statement. |support_hooks|

        .. versionadded:: 0.2.0

        Parameters
        ----------
        statement : str

            Statement to be executed.
        """

        statement = clear_statement(statement)

        log.info("|%s| Executing statement:", self.__class__.__name__)
        log_lines(log, statement)

        with SparkMetricsRecorder(self.spark) as recorder:
            try:
                with override_job_description(self.spark, f"{self}.execute()"):
                    self._execute_sql(statement).collect()
            except Exception:
                log.error("|%s| Execution failed", self.__class__.__name__)
                metrics = recorder.metrics()
                if log.isEnabledFor(logging.DEBUG) and not metrics.is_empty:
                    # as SparkListener results are not guaranteed to be received in time,
                    # some metrics may be missing. To avoid confusion, log only in debug, and with a notice
                    log.info("|%s| Recorded metrics (some values may be missing!):", self.__class__.__name__)
                    log_lines(log, str(metrics), level=logging.DEBUG)
                raise

            log.info("|%s| Execution succeeded", self.__class__.__name__)

            metrics = recorder.metrics()
            if log.isEnabledFor(logging.DEBUG) and not metrics.is_empty:
                # as SparkListener results are not guaranteed to be received in time,
                # some metrics may be missing. To avoid confusion, log only in debug, and with a notice
                log.info("|%s| Recorded metrics (some values may be missing!):", self.__class__.__name__)
                log_lines(log, str(metrics), level=logging.DEBUG)

    @slot
    def write_df_to_target(
        self,
        df: DataFrame,
        target: str,
        options: HiveWriteOptions | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)

        # https://stackoverflow.com/a/72747050
        if self._target_exist(target) and write_options.if_exists != HiveTableExistBehavior.REPLACE_ENTIRE_TABLE:
            if write_options.if_exists == HiveTableExistBehavior.ERROR:
                raise ValueError("Operation stopped due to Hive.WriteOptions(if_exists='error')")
            elif write_options.if_exists == HiveTableExistBehavior.IGNORE:
                log.info(
                    "|%s| Skip writing to existing table because of Hive.WriteOptions(if_exists='ignore')",
                    self.__class__.__name__,
                )
                return
            # using saveAsTable on existing table does not handle
            # spark.sql.sources.partitionOverwriteMode=dynamic, so using insertInto instead.
            self._insert_into(df, target, options)
        else:
            # if someone needs to recreate the entire table using new set of options, like partitionBy or bucketBy,
            # if_exists="replace_entire_table" should be used
            self._save_as_table(df, target, options)

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
    ) -> DataFrame:
        query = self.dialect.get_sql_query(
            table=source,
            columns=columns,
            where=self.dialect.apply_window(where, window),
            hint=hint,
            limit=limit,
        )

        return self.sql(query)

    @slot
    def get_df_schema(
        self,
        source: str,
        columns: list[str] | None = None,
    ) -> StructType:
        log.info("|%s| Fetching schema of table %r ...", self.__class__.__name__, source)
        query = self.dialect.get_sql_query(source, columns=columns, where=0, compact=True)

        log.debug("|%s| Executing SQL query:", self.__class__.__name__)
        log_lines(log, query, level=logging.DEBUG)

        df = self._execute_sql(query)
        log.info("|%s| Schema fetched.", self.__class__.__name__)
        return df.schema

    @slot
    def get_min_max_values(
        self,
        source: str,
        window: Window,
        hint: Any | None = None,
        where: Any | None = None,
    ) -> tuple[Any, Any]:
        log.info("|%s| Getting min and max values for expression %r ...", self.__class__.__name__, window.expression)

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
            hint=hint,
        )

        log.info("|%s| Executing SQL query:", self.__class__.__name__)
        log_lines(log, query)

        df = self._execute_sql(query)
        row = df.collect()[0]
        min_value = row["min"]
        max_value = row["max"]

        log.info("|%s| Received values:", self.__class__.__name__)
        log_with_indent(log, "MIN(%s) = %r", window.expression, min_value)
        log_with_indent(log, "MAX(%s) = %r", window.expression, max_value)

        return min_value, max_value

    @validator("cluster")
    def _validate_cluster_name(cls, cluster):
        log.debug("|%s| Normalizing cluster %r name...", cls.__name__, cluster)
        validated_cluster = cls.Slots.normalize_cluster_name(cluster) or cluster
        if validated_cluster != cluster:
            log.debug("|%s|   Got %r", cls.__name__)

        log.debug("|%s| Checking if cluster %r is a known cluster...", cls.__name__, validated_cluster)
        known_clusters = cls.Slots.get_known_clusters()
        if known_clusters and validated_cluster not in known_clusters:
            raise ValueError(
                f"Cluster {validated_cluster!r} is not in the known clusters list: {sorted(known_clusters)!r}",
            )

        return validated_cluster

    def _execute_sql(self, query: str) -> DataFrame:
        return self.spark.sql(query)

    def _sort_df_columns_like_table(self, table: str, df_columns: list[str]) -> list[str]:
        # Hive is inserting columns by the order, not by their name
        # so if you're inserting dataframe with columns B, A, C to table with columns A, B, C, data will be damaged
        # so it is important to sort columns in dataframe to match columns in the table.

        table_columns = self.spark.table(table).columns

        # But names could have different cases, this should not cause errors
        table_columns_normalized = [column.casefold() for column in table_columns]
        df_columns_normalized = [column.casefold() for column in df_columns]

        missing_columns_df = [column for column in df_columns_normalized if column not in table_columns_normalized]
        missing_columns_table = [column for column in table_columns_normalized if column not in df_columns_normalized]

        if missing_columns_df or missing_columns_table:
            missing_columns_df_message = ""
            if missing_columns_df:
                missing_columns_df_message = f"""
                    These columns present only in dataframe:
                        {missing_columns_df!r}
                    """

            missing_columns_table_message = ""
            if missing_columns_table:
                missing_columns_table_message = f"""
                    These columns present only in table:
                        {missing_columns_table!r}
                    """

            raise ValueError(
                dedent(
                    f"""
                    Inconsistent columns between a table and the dataframe!

                    Table {table!r} has columns:
                        {table_columns!r}

                    Dataframe has columns:
                        {df_columns!r}
                    {missing_columns_df_message}{missing_columns_table_message}
                    """,
                ).strip(),
            )

        return sorted(df_columns, key=lambda column: table_columns_normalized.index(column.casefold()))

    def _target_exist(self, name: str) -> bool:
        from pyspark.sql.functions import col, lower

        log.info("|%s| Checking if table %r exists ...", self.__class__.__name__, name)

        # Do not use SELECT * FROM table, because it may fail if users have no permissions,
        # or Hive Metastore is overloaded.
        # Also we ignore VIEW's as they are not insertable.
        schema, table = name.split(".", maxsplit=1)
        query = f"SHOW TABLES IN {schema} LIKE '{table}'"

        log.debug("|%s| Executing SQL query:", self.__class__.__name__)
        log_lines(log, query, level=logging.DEBUG)

        # Spark normalizes table names to lowercase, so we do the same
        df = self._execute_sql(query).where(lower(col("tableName")) == table.lower())
        if df.take(1):
            log.info("|%s| Table %r exists.", self.__class__.__name__, name)
            return True

        log.info("|%s| Table %r does not exist.", self.__class__.__name__, name)
        return False

    def _insert_into(
        self,
        df: DataFrame,
        table: str,
        options: HiveWriteOptions | dict | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)
        unsupported_options = self._format_write_options(write_options)
        if unsupported_options:
            log.warning(
                "|%s| User-specified options %r are ignored while inserting into existing table. "
                "Using only table parameters from Hive metastore",
                self.__class__.__name__,
                unsupported_options,
            )

        # Hive is inserting data to table by column position, not by name
        # So we should sort columns according their order in the existing table
        # instead of using order from the dataframe
        columns = self._sort_df_columns_like_table(table, df.columns)
        if columns != df.columns:
            df = df.select(*columns)

        # Writer option "partitionOverwriteMode" was added to Spark only in 2.4.0
        # so using a workaround with patching Spark config and then setting up the previous value
        with inject_spark_param(self.spark.conf, PARTITION_OVERWRITE_MODE_PARAM, "dynamic"):
            overwrite = write_options.if_exists != HiveTableExistBehavior.APPEND

            log.info("|%s| Inserting data into existing table %r ...", self.__class__.__name__, table)
            df.write.insertInto(table, overwrite=overwrite)

        log.info("|%s| Data is successfully inserted into table %r.", self.__class__.__name__, table)

    def _format_write_options(self, write_options: HiveWriteOptions) -> dict:
        options_dict = write_options.dict(
            by_alias=True,
            exclude_unset=True,
            exclude={"if_exists"},
        )

        if isinstance(write_options.format, (WriteOnlyFileFormat, ReadWriteFileFormat)):
            options_dict["format"] = write_options.format.name
            options_dict.update(write_options.format.dict(exclude={"name"}))

        return options_dict

    def _save_as_table(
        self,
        df: DataFrame,
        table: str,
        options: HiveWriteOptions | dict | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)

        writer = df.write
        for method, value in write_options.dict(  # noqa: WPS352
            by_alias=True,
            exclude_none=True,
            exclude={"if_exists", "format"},
        ).items():
            if hasattr(writer, method):
                if isinstance(value, Iterable) and not isinstance(value, str):
                    writer = getattr(writer, method)(*value)
                else:
                    writer = getattr(writer, method)(value)
            else:
                writer = writer.option(method, value)

        # deserialize passed OCR(), Parquet(), CSV(), etc. file formats
        if isinstance(write_options.format, BaseWritableFileFormat):
            writer = write_options.format.apply_to_writer(writer)
        elif isinstance(write_options.format, str):
            writer = writer.format(write_options.format)

        mode = "append" if write_options.if_exists == HiveTableExistBehavior.APPEND else "overwrite"

        log.info("|%s| Saving data to a table %r ...", self.__class__.__name__, table)
        writer.mode(mode).saveAsTable(table)

        log.info("|%s| Table %r is successfully created.", self.__class__.__name__, table)
