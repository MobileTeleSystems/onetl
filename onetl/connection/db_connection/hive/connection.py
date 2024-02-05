# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from textwrap import dedent
from typing import TYPE_CHECKING, Any, ClassVar, Iterable

from etl_entities.instance import Cluster
from pydantic import validator

from onetl._internal import clear_statement
from onetl._util.spark import inject_spark_param
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.hive.dialect import HiveDialect
from onetl.connection.db_connection.hive.options import (
    HiveLegacyOptions,
    HiveTableExistBehavior,
    HiveWriteOptions,
)
from onetl.connection.db_connection.hive.slots import HiveSlots
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

    You don't need a Hive server to use this connector.

    .. dropdown:: Version compatibility

        * Hive metastore version: 0.12 - 3.1.2 (may require to add proper .jar file explicitly)
        * Spark versions: 2.3.x - 3.5.x
        * Java versions: 8 - 20

    .. warning::

        To use Hive connector you should have PySpark installed (or injected to ``sys.path``)
        BEFORE creating the connector instance.

        You can install PySpark as follows:

        .. code:: bash

            pip install onetl[spark]  # latest PySpark version

            # or
            pip install onetl pyspark=3.5.0  # pass specific PySpark version

        See :ref:`install-spark` installation instruction for more details.

    .. warning::

        This connector requires some additional configuration files to be present (``hive-site.xml`` and so on),
        as well as .jar files with Hive MetaStore client.

        See `Spark Hive Tables documentation <https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html>`_
        and `this guide <https://dataedo.com/docs/apache-spark-hive-metastore>`_ for more details.

    .. note::

        Most of Hadoop instances use Kerberos authentication. In this case, you should call ``kinit``
        **BEFORE** starting Spark session to generate Kerberos ticket. See :ref:`install-kerberos`.

        In case of creating session with ``"spark.master": "yarn"``, you should also pass some additional options
        to Spark session, allowing executors to generate their own Kerberos tickets to access HDFS.
        See `Spark security documentation <https://spark.apache.org/docs/latest/security.html#kerberos>`_
        for more details.

    Parameters
    ----------
    cluster : str
        Cluster name. Used for HWM and lineage.

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session with Hive metastore support enabled

    Examples
    --------

    Hive connection initialization

    .. code:: python

        from onetl.connection import Hive
        from pyspark.sql import SparkSession

        # Create Spark session
        spark = SparkSession.builder.appName("spark-app-name").enableHiveSupport().getOrCreate()

        # Create connection
        hive = Hive(cluster="rnd-dwh", spark=spark).check()

    Hive connection initialization with Kerberos support

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
            self._execute_sql(self._CHECK_QUERY).limit(1).collect()
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

            SQL query to be executed, like:

            * ``SELECT ... FROM ...``
            * ``WITH ... AS (...) SELECT ... FROM ...``
            * ``SHOW ...`` queries are also supported, like ``SHOW TABLES``

        Returns
        -------
        df : pyspark.sql.dataframe.DataFrame

            Spark dataframe

        Examples
        --------

        Read data from Hive table:

        .. code:: python

            connection = Hive(cluster="rnd-dwh", spark=spark)

            df = connection.sql("SELECT * FROM mytable")
        """

        query = clear_statement(query)

        log.info("|%s| Executing SQL query:", self.__class__.__name__)
        log_lines(log, query)

        df = self._execute_sql(query)
        log.info("|Spark| DataFrame successfully created from SQL statement")
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

            Statement to be executed, like:

            DML statements:

            * ``INSERT INTO target_table SELECT * FROM source_table``
            * ``TRUNCATE TABLE mytable``

            DDL statements:

            * ``CREATE TABLE mytable (...)``
            * ``ALTER TABLE mytable ...``
            * ``DROP TABLE mytable``
            * ``MSCK REPAIR TABLE mytable``

            The exact list of supported statements depends on Hive version,
            for example some new versions support ``CREATE FUNCTION`` syntax.

        Examples
        --------

        Create table:

        .. code:: python

            connection = Hive(cluster="rnd-dwh", spark=spark)

            connection.execute(
                "CREATE TABLE mytable (id NUMBER, data VARCHAR) PARTITIONED BY (date DATE)"
            )

        Drop table partition:

        .. code:: python

            connection = Hive(cluster="rnd-dwh", spark=spark)

            connection.execute("ALTER TABLE mytable DROP PARTITION(date='2023-02-01')")
        """

        statement = clear_statement(statement)

        log.info("|%s| Executing statement:", self.__class__.__name__)
        log_lines(log, statement)

        self._execute_sql(statement).collect()
        log.info("|%s| Call succeeded", self.__class__.__name__)

    @slot
    def write_df_to_target(
        self,
        df: DataFrame,
        target: str,
        options: HiveWriteOptions | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)

        try:
            self.get_df_schema(target)
            table_exists = True

            log.info("|%s| Table %r already exists", self.__class__.__name__, target)
        except Exception:
            table_exists = False

        # https://stackoverflow.com/a/72747050
        if table_exists and write_options.if_exists != HiveTableExistBehavior.REPLACE_ENTIRE_TABLE:
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

        log.info("|%s| Executing SQL query (on driver):", self.__class__.__name__)
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

    def _insert_into(
        self,
        df: DataFrame,
        table: str,
        options: HiveWriteOptions | dict | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)

        unsupported_options = write_options.dict(by_alias=True, exclude_unset=True, exclude={"if_exists"})
        if unsupported_options:
            log.warning(
                "|%s| Options %r are not supported while inserting into existing table, ignoring",
                self.__class__.__name__,
                unsupported_options,
            )

        # Hive is inserting data to table by column position, not by name
        # So we should sort columns according their order in the existing table
        # instead of using order from the dataframe
        columns = self._sort_df_columns_like_table(table, df.columns)
        writer = df.select(*columns).write

        # Writer option "partitionOverwriteMode" was added to Spark only in 2.4.0
        # so using a workaround with patching Spark config and then setting up the previous value
        with inject_spark_param(self.spark.conf, PARTITION_OVERWRITE_MODE_PARAM, "dynamic"):
            overwrite = write_options.if_exists != HiveTableExistBehavior.APPEND

            log.info("|%s| Inserting data into existing table %r ...", self.__class__.__name__, table)
            writer.insertInto(table, overwrite=overwrite)

        log.info("|%s| Data is successfully inserted into table %r.", self.__class__.__name__, table)

    def _save_as_table(
        self,
        df: DataFrame,
        table: str,
        options: HiveWriteOptions | dict | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)

        writer = df.write
        for method, value in write_options.dict(by_alias=True, exclude_none=True, exclude={"if_exists"}).items():
            # <value> is the arguments that will be passed to the <method>
            # format orc, parquet methods and format simultaneously
            if hasattr(writer, method):
                if isinstance(value, Iterable) and not isinstance(value, str):
                    writer = getattr(writer, method)(*value)  # noqa: WPS220
                else:
                    writer = getattr(writer, method)(value)  # noqa: WPS220
            else:
                writer = writer.option(method, value)

        mode = "append" if write_options.if_exists == HiveTableExistBehavior.APPEND else "overwrite"

        log.info("|%s| Saving data to a table %r ...", self.__class__.__name__, table)
        writer.mode(mode).saveAsTable(table)

        log.info("|%s| Table %r is successfully created.", self.__class__.__name__, table)
