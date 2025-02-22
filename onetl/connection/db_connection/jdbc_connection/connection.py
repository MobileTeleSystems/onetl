# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import secrets
import warnings
from typing import TYPE_CHECKING, Any, ClassVar

try:
    from pydantic.v1 import SecretStr, validator
except (ImportError, AttributeError):
    from pydantic import SecretStr, validator  # type: ignore[no-redef, assignment]

from onetl._util.java import try_import_java_class
from onetl._util.spark import override_job_description
from onetl._util.sql import clear_statement
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.jdbc_connection.dialect import JDBCDialect
from onetl.connection.db_connection.jdbc_connection.options import (
    JDBCLegacyOptions,
    JDBCPartitioningMode,
    JDBCReadOptions,
    JDBCSQLOptions,
    JDBCTableExistBehavior,
    JDBCWriteOptions,
)
from onetl.connection.db_connection.jdbc_mixin import JDBCMixin
from onetl.connection.db_connection.jdbc_mixin.options import (
    JDBCExecuteOptions,
    JDBCFetchOptions,
)
from onetl.connection.db_connection.jdbc_mixin.options import (
    JDBCOptions as JDBCMixinOptions,
)
from onetl.exception import MISSING_JVM_CLASS_MSG
from onetl.hooks import slot, support_hooks
from onetl.hwm import Window
from onetl.log import log_lines, log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = logging.getLogger(__name__)

# parameters accepted by spark.read.jdbc method:
#  spark.read.jdbc(
#    url, table, column, lowerBound, upperBound, numPartitions, predicates
#    properties:  { "user" : "SYSTEM", "password" : "mypassword", ... })
READ_TOP_LEVEL_OPTIONS = frozenset(("url", "column", "lower_bound", "upper_bound", "num_partitions", "predicates"))

# parameters accepted by spark.write.jdbc method:
#   spark.write.jdbc(
#     url, table, mode,
#     properties:  { "user" : "SYSTEM", "password" : "mypassword", ... })
WRITE_TOP_LEVEL_OPTIONS = frozenset("url")


@support_hooks
class JDBCConnection(JDBCMixin, DBConnection):  # noqa: WPS338
    user: str
    password: SecretStr

    DRIVER: ClassVar[str]
    _CHECK_QUERY: ClassVar[str] = "SELECT 1"

    JDBCOptions = JDBCMixinOptions
    FetchOptions = JDBCFetchOptions
    ExecuteOptions = JDBCExecuteOptions
    Dialect = JDBCDialect
    ReadOptions = JDBCReadOptions
    SQLOptions = JDBCSQLOptions
    WriteOptions = JDBCWriteOptions
    Options = JDBCLegacyOptions

    @validator("spark")
    def _check_java_class_imported(cls, spark):
        try:
            try_import_java_class(spark, cls.DRIVER)
        except Exception as e:
            msg = MISSING_JVM_CLASS_MSG.format(
                java_class=cls.DRIVER,
                package_source=cls.__name__,
                args="",
            )
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Missing Java class", exc_info=e, stack_info=True)
            raise ValueError(msg) from e
        return spark

    @slot
    def check(self):
        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()  # type: ignore

        log.debug("|%s| Executing SQL query:", self.__class__.__name__)
        log_lines(log, self._CHECK_QUERY, level=logging.DEBUG)

        try:
            with override_job_description(self.spark, f"{self}.check()"):
                self._query_optional_on_driver(self._CHECK_QUERY, self.FetchOptions(fetchsize=1))
                self._query_on_executor(self._CHECK_QUERY, self.SQLOptions(fetchsize=1)).collect()
            log.info("|%s| Connection is available.", self.__class__.__name__)
        except Exception as e:
            log.exception("|%s| Connection is unavailable", self.__class__.__name__)
            raise RuntimeError("Connection is unavailable") from e

        return self

    @slot
    def sql(
        self,
        query: str,
        options: JDBCSQLOptions | dict | None = None,
    ) -> DataFrame:
        """
        **Lazily** execute SELECT statement **on Spark executor** and return DataFrame. |support_hooks|

        Same as ``spark.read.jdbc(query)``.

        .. versionadded:: 0.2.0

        Parameters
        ----------
        query : str

            SQL query to be executed.

        options : dict, :obj:`~SQLOptions`, default: ``None``

            Spark options to be used while fetching data.

        Returns
        -------
        df : pyspark.sql.dataframe.DataFrame

            Spark dataframe

        """

        if isinstance(options, JDBCReadOptions):
            msg = "Using `ReadOptions` for `sql` method is deprecated, use `SQLOptions` instead."
            warnings.warn(msg, UserWarning, stacklevel=3)
            options = self.SQLOptions.parse_obj(options.dict(exclude={"partitioning_mode"}, exclude_none=True))

        query = clear_statement(query)

        log.info("|%s| Detected dialect: '%s'", self.__class__.__name__, self._get_spark_dialect_name())
        log.info("|%s| Executing SQL query (on executor):", self.__class__.__name__)
        log_lines(log, query)

        try:
            with override_job_description(self.spark, f"{self}.sql()"):
                df = self._query_on_executor(query, self.SQLOptions.parse(options))
        except Exception:
            log.error("|%s| Query failed!", self.__class__.__name__)
            raise

        log.info("|Spark| DataFrame successfully created from SQL statement")
        return df

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
        options: JDBCReadOptions | None = None,
    ) -> DataFrame:
        if isinstance(options, JDBCLegacyOptions):
            raw_options = self.ReadOptions.parse(options.dict(exclude_unset=True))
        else:
            raw_options = self.ReadOptions.parse(options)

        read_options = self._set_lower_upper_bound(
            table=source,
            where=where,
            hint=hint,
            options=raw_options,
        )

        new_columns = columns or ["*"]
        alias: str | None = None

        if read_options.partition_column:
            if read_options.partitioning_mode == JDBCPartitioningMode.MOD:
                partition_column = self.dialect.get_partition_column_mod(
                    read_options.partition_column,
                    read_options.num_partitions,
                )
            elif read_options.partitioning_mode == JDBCPartitioningMode.HASH:
                partition_column = self.dialect.get_partition_column_hash(
                    read_options.partition_column,
                    read_options.num_partitions,
                )
            else:
                partition_column = read_options.partition_column

            # hack to avoid column name verification
            # in the spark, the expression in the partitioning of the column must
            # have the same name as the field in the table ( 2.4 version )
            # https://github.com/apache/spark/pull/21379
            alias = "generated_" + secrets.token_hex(5)
            alias_escaped = self.dialect.escape_column(alias)
            aliased_column = self.dialect.aliased(partition_column, alias_escaped)
            read_options = read_options.copy(update={"partition_column": alias_escaped})
            new_columns.append(aliased_column)

        where = self.dialect.apply_window(where, window)
        query = self.dialect.get_sql_query(
            table=source,
            columns=new_columns,
            where=where,
            hint=hint,
            limit=limit,
        )

        log.info("|%s| Detected dialect: '%s'", self.__class__.__name__, self._get_spark_dialect_name())
        log.info("|%s| Executing SQL query (on executor):", self.__class__.__name__)
        log_lines(log, query)

        result = self._query_on_executor(query, self.ReadOptions.parse(read_options))

        log.info("|Spark| DataFrame successfully created from SQL statement ")
        if alias:
            result = result.drop(alias)

        return result

    @slot
    def write_df_to_target(
        self,
        df: DataFrame,
        target: str,
        options: JDBCWriteOptions | None = None,
    ) -> None:
        if isinstance(options, JDBCLegacyOptions):
            write_options = self.WriteOptions.parse(options.dict(exclude_unset=True))
        else:
            write_options = self.WriteOptions.parse(options)

        jdbc_properties = self._get_jdbc_properties(write_options, exclude={"if_exists"}, exclude_none=True)

        mode = (
            "overwrite"
            if write_options.if_exists == JDBCTableExistBehavior.REPLACE_ENTIRE_TABLE
            else write_options.if_exists.value
        )
        log.info("|%s| Saving data to a table %r", self.__class__.__name__, target)
        log.info("|%s| Detected dialect: '%s'", self.__class__.__name__, self._get_spark_dialect_name())
        df.write.format("jdbc").mode(mode).options(dbtable=target, **jdbc_properties).save()
        log.info("|%s| Table %r successfully written", self.__class__.__name__, target)

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
        read_options = self._exclude_partition_options(self.ReadOptions.parse(options), fetchsize=0)

        log.debug("|%s| Executing SQL query (on driver):", self.__class__.__name__)
        log_lines(log, query, level=logging.DEBUG)

        df = self._query_on_driver(query, read_options)
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
        log.info("|%s| Getting min and max values for expression %r ...", self.__class__.__name__, window.expression)
        read_options = self._exclude_partition_options(self.ReadOptions.parse(options), fetchsize=1)

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

        log.info("|%s| Executing SQL query (on driver):", self.__class__.__name__)
        log_lines(log, query)

        df = self._query_on_driver(query, read_options)
        row = df.collect()[0]
        min_value = row["min"]
        max_value = row["max"]

        log.info("|%s| Received values:", self.__class__.__name__)
        log_with_indent(log, "MIN(%s) = %r", window.expression, min_value)
        log_with_indent(log, "MAX(%s) = %r", window.expression, max_value)

        return min_value, max_value

    def _query_on_executor(
        self,
        query: str,
        options: JDBCSQLOptions | JDBCReadOptions,
    ) -> DataFrame:
        jdbc_properties = self._get_jdbc_properties(options, exclude_none=True)
        return self.spark.read.format("jdbc").options(dbtable=f"({query}) T", **jdbc_properties).load()

    def _exclude_partition_options(
        self,
        options: JDBCReadOptions,
        fetchsize: int,
    ) -> JDBCFetchOptions:
        return self.FetchOptions.parse(
            options.copy(
                update={"fetchsize": fetchsize},
                exclude={"partition_column", "lower_bound", "upper_bound", "num_partitions", "partitioning_mode"},
            ).dict(),
        )

    def _set_lower_upper_bound(
        self,
        table: str,
        hint: str | None,
        where: str | None,
        options: JDBCReadOptions,
    ) -> JDBCReadOptions:
        """
        Determine values of upperBound and lowerBound options
        """
        if not options.partition_column:
            return options

        missing_values: list[str] = []

        is_missed_lower_bound = options.lower_bound is None
        is_missed_upper_bound = options.upper_bound is None

        if is_missed_lower_bound:
            missing_values.append("lowerBound")

        if is_missed_upper_bound:
            missing_values.append("upperBound")

        if not missing_values:
            return options

        log.warning(
            "|%s| Passed numPartitions = %d, but values %r are not set. "
            "They will be detected automatically based on values in partitionColumn %r",
            self.__class__.__name__,
            options.num_partitions,
            missing_values,
            options.partition_column,
        )

        min_partition_value, max_partition_value = self.get_min_max_values(
            source=table,
            window=Window(options.partition_column),
            where=where,
            hint=hint,
            options=options,
        )

        # The sessionInitStatement parameter is removed because it only needs to be applied once.
        return options.copy(
            exclude={"session_init_statement"},
            update={
                "lower_bound": options.lower_bound if not is_missed_lower_bound else min_partition_value,
                "upper_bound": options.upper_bound if not is_missed_upper_bound else max_partition_value,
            },
        )

    def _log_parameters(self):
        super()._log_parameters()
        log_with_indent(log, "jdbc_url = %r", self.jdbc_url)
