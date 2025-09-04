# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Iterable

from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.version import Version
from onetl.connection.db_connection.iceberg.dialect import IcebergDialect
from onetl.connection.db_connection.iceberg.extra import IcebergExtra
from onetl.connection.db_connection.iceberg.options import (
    IcebergReadOptions,
    IcebergTableExistBehavior,
    IcebergWriteOptions,
)
from onetl.exception import MISSING_JVM_CLASS_MSG

try:
    from pydantic.v1 import validator
except (ImportError, AttributeError):
    from pydantic import validator  # type: ignore[no-redef, assignment]

from onetl._metrics.recorder import SparkMetricsRecorder
from onetl._util.spark import get_spark_version, override_job_description
from onetl._util.sql import clear_statement
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.hooks import slot, support_hooks
from onetl.hwm import Window
from onetl.log import log_lines, log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType


log = logging.getLogger(__name__)


@support_hooks
class Iceberg(DBConnection):
    """Iceberg connection. |support_hooks|

    .. danger::

        This is an alpha version of connector, it's behavior may change in the future.

    .. seealso::

        Before using this connector please take into account :ref:`iceberg-prerequisites`

    .. versionadded:: 0.14.0

    Parameters
    ----------
    catalog_name : str
        Catalog name

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session

    extra : dict | None, default: ``None``
        A dictionary of additional properties to be used when configuring Iceberg catalog.

        These are Iceberg-specific properties that control behavior of the catalog.
        See `Iceberg Spark configuration documentation <https://iceberg.apache.org/docs/latest/spark-configuration/>`_

        Pass properties **without catalog prefix**. For example:

        .. code:: python

            extra = {
                "type": "hadoop",
                "warehouse": "file:///path/to/warehouse",
            }

        This will be translated to:

        .. code:: ini

            spark.sql.catalog.my_catalog = 'org.apache.iceberg.spark.SparkCatalog'
            spark.sql.catalog.my_catalog.type = 'hadoop'
            spark.sql.catalog.my_catalog.warehouse = 'file:///path/to/warehouse'

    Examples
    --------

    **REST catalog + S3 warehouse**

    .. code:: python

        from onetl.connection import Iceberg
        from pyspark.sql import SparkSession

        maven_packages = [
            *Iceberg.get_packages(package_version="1.9.2", spark_version="3.5"),
            *SparkS3.get_packages(spark_version="3.5.6"),
        ]
        exclude_packages = SparkS3.get_exclude_packages()
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .config("spark.jars.excludes", ",".join(exclude_packages))
            .getOrCreate()
        )

        # Create connection
        iceberg = Iceberg(
            catalog_name="my_catalog",
            spark=spark,
            extra={
                "type": "rest",
                "uri": "http://localhost:8080",
                "warehouse": "s3a://bucket/",
                "hadoop.fs.s3a.endpoint": "http://localhost:9010",
                "hadoop.fs.s3a.access.key": "access_key",
                "hadoop.fs.s3a.secret.key": "secret_key",
                "hadoop.fs.s3a.path.style.access": "true",
            },
        )


    **Hadoop catalog + HDFS warehouse**

    .. code:: python

        from onetl.connection import Iceberg
        from pyspark.sql import SparkSession

        maven_packages = Iceberg.get_packages(package_version="1.9.2", spark_version="3.5")
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        # Create connection
        iceberg = Iceberg(
            catalog_name="my_catalog",
            spark=spark,
            extra={
                "type": "hadoop",
                "warehouse": "hdfs://namenode:8020/warehouse/path",
            },
        )
    """

    catalog_name: str
    extra: IcebergExtra = IcebergExtra()

    ReadOptions = IcebergReadOptions
    WriteOptions = IcebergWriteOptions

    Dialect = IcebergDialect

    @property
    def _check_query(self) -> str:
        return f"SHOW NAMESPACES IN {self.catalog_name}"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.spark.conf.set(
            f"spark.sql.catalog.{self.catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        for k, v in self.extra.dict().items():
            self.spark.conf.set(f"spark.sql.catalog.{self.catalog_name}.{k}", v)

    @slot
    @classmethod
    def get_packages(
        cls,
        package_version: str,
        spark_version: str,
        scala_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        See `Maven package index <https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-spark>`_
        for all available packages.

        Parameters
        ----------
        package_version : str
            Iceberg package version in format ``major.minor.patch``.

        spark_version : str
            Spark version in format ``major.minor``.

        scala_version : str, optional
            Scala version in format ``major.minor``.

            If ``None``, ``spark_version`` is used to determine Scala version.

        Returns
        -------
        list[str]
            List of Maven coordinates.

        Examples
        --------
        .. code:: python

            from onetl.connection import Iceberg

            # Note: Iceberg 1.9.2 requires Java 11+
            Iceberg.get_packages(package_version="1.9.2", spark_version="3.5")
        """

        version = Version(package_version).min_digits(3)
        spark_ver = Version(spark_version).min_digits(2)
        scala_ver = Version(scala_version).min_digits(2) if scala_version else get_default_scala_version(spark_ver)
        return [
            f"org.apache.iceberg:iceberg-spark-runtime-{spark_ver.format('{0}.{1}')}_{scala_ver.format('{0}.{1}')}:{version}",
        ]

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.catalog_name}"

    def __str__(self):
        return f"{self.__class__.__name__}[{self.catalog_name}]"

    @validator("spark")
    def _check_java_class_imported(cls, spark: SparkSession) -> SparkSession:
        java_class = "org.apache.iceberg.spark.SparkSessionCatalog"

        try:
            try_import_java_class(spark, java_class)
        except Exception as e:
            spark_version = get_spark_version(spark).format("{0}.{1}.{2}")
            msg = MISSING_JVM_CLASS_MSG.format(
                java_class=java_class,
                package_source=cls.__class__.__name__,
                args=f"spark_version='{spark_version}'",
            )
            raise ValueError(msg) from e

        return spark

    @slot
    def check(self):
        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()

        log.debug("|%s| Executing SQL query:", self.__class__.__name__)
        log_lines(log, self._check_query, level=logging.DEBUG)

        try:
            with override_job_description(self.spark, f"{self}.check()"):
                self._execute_sql(self._check_query).take(1)
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
        options: IcebergWriteOptions | None = None,
    ) -> None:
        target = self._normalize_table_name(target)
        write_options = self.WriteOptions.parse(options)

        table_exists = self._target_exist(target)

        if not table_exists or write_options.if_exists == IcebergTableExistBehavior.REPLACE_ENTIRE_TABLE:
            self._save_as_table(df, target, options)
            return

        if write_options.if_exists == IcebergTableExistBehavior.ERROR:
            raise ValueError("Operation stopped due to Iceberg.WriteOptions(if_exists='error')")

        if write_options.if_exists == IcebergTableExistBehavior.IGNORE:
            log.info(
                "|%s| Skip writing to existing table because of Iceberg.WriteOptions(if_exists='ignore')",
                self.__class__.__name__,
            )
            return

        self._insert_into(df, target, options)

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
        options: IcebergReadOptions | None = None,
    ) -> DataFrame:
        query = self.dialect.get_sql_query(
            table=self._normalize_table_name(source),
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
        source = self._normalize_table_name(source)
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

        source = self._normalize_table_name(source)
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

    def _execute_sql(self, query: str) -> DataFrame:
        return self.spark.sql(query)

    def _target_exist(self, name: str) -> bool:
        from pyspark.sql.functions import col, lower

        log.info("|%s| Checking if table %r exists ...", self.__class__.__name__, name)

        # Do not use SELECT * FROM table, because it may fail if users have no permissions
        # Also we ignore VIEW's as they are not insertable.
        schema, table = name.rsplit(".", maxsplit=1)
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

    def _save_as_table(
        self,
        df: DataFrame,
        table: str,
        options: IcebergWriteOptions | dict | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)

        writer = df.writeTo(table).using("iceberg")
        for method, value in self._format_write_options(write_options).items():
            if hasattr(writer, method):
                if isinstance(value, Iterable) and not isinstance(value, str):
                    writer = getattr(writer, method)(*value)
                else:
                    writer = getattr(writer, method)(value)
            else:
                writer = writer.option(method, value)

        log.info("|%s| Saving data to a table %r ...", self.__class__.__name__, table)
        writer.createOrReplace()
        log.info("|%s| Table %r is successfully created.", self.__class__.__name__, table)

    def _insert_into(
        self,
        df: DataFrame,
        table: str,
        options: IcebergWriteOptions | dict | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)
        unsupported_options = self._format_write_options(write_options)
        if unsupported_options:
            log.warning(
                "|%s| User-specified options %r are ignored while inserting into existing table. "
                "Using only Iceberg table parameters",
                self.__class__.__name__,
                unsupported_options,
            )

        log.info("|%s| Inserting data into existing table %r ...", self.__class__.__name__, table)

        writer = df.writeTo(table).using("iceberg")

        if write_options.if_exists == IcebergTableExistBehavior.APPEND:
            writer.append()
        else:
            writer.overwritePartitions()

        log.info("|%s| Data is successfully inserted into table %r.", self.__class__.__name__, table)

    def _format_write_options(self, write_options: IcebergWriteOptions) -> dict:
        return write_options.dict(
            by_alias=True,
            exclude_unset=True,
            exclude={"if_exists"},
        )

    def _normalize_table_name(self, table: str) -> str:
        return f"{self.catalog_name}.{table}"
