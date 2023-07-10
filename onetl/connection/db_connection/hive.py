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
from enum import Enum
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Tuple, Union

from deprecated import deprecated
from etl_entities.instance import Cluster
from pydantic import root_validator, validator

from onetl._internal import clear_statement, get_sql_query, to_camel  # noqa: WPS436
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.dialect_mixins import (
    SupportColumnsList,
    SupportDfSchemaNone,
    SupportHintStr,
    SupportHWMExpressionStr,
    SupportWhereStr,
)
from onetl.connection.db_connection.dialect_mixins.support_table_with_dbschema import (
    SupportTableWithDBSchema,
)
from onetl.hooks import slot, support_hooks
from onetl.hwm import Statement
from onetl.impl import GenericOptions
from onetl.log import log_lines, log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

PARTITION_OVERWRITE_MODE_PARAM = "spark.sql.sources.partitionOverwriteMode"
log = logging.getLogger(__name__)


class HiveWriteMode(str, Enum):
    APPEND = "append"
    OVERWRITE_TABLE = "overwrite_table"
    OVERWRITE_PARTITIONS = "overwrite_partitions"

    def __str__(self):
        return str(self.value)

    @classmethod  # noqa: WPS120
    def _missing_(cls, value: object):  # noqa: WPS120
        if str(value) == "overwrite":
            log.warning(
                "Mode `overwrite` is deprecated since v0.4.0 and will be removed in v1.0.0, "
                "use `overwrite_partitions` instead",
            )
            return cls.OVERWRITE_PARTITIONS


@support_hooks
class Hive(DBConnection):
    """Spark connection with Hive MetaStore support. |support_hooks|

    You don't need a Hive server to use this connector.

    .. dropdown:: Version compatibility

        * Hive metastore version: 0.12 - 3.1.2 (may require to add proper .jar file explicitly)
        * Spark versions: 2.3.x - 3.4.x
        * Java versions: 8 - 17

    .. warning::

        To use Hive connector you should have PySpark installed (or injected to ``sys.path``)
        BEFORE creating the connector instance.

        You can install PySpark as follows:

        .. code:: bash

            pip install onetl[spark]  # latest PySpark version

            # or
            pip install onetl pyspark=3.4.1  # pass specific PySpark version

        See :ref:`spark-install` instruction for more details.

    .. warning::

        This connector requires some additional configuration files to be present (``hive-site.xml`` and so on),
        as well as .jar files with Hive MetaStore client.

        See `Spark Hive Tables documentation <https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html>`_
        and `this guide <https://dataedo.com/docs/apache-spark-hive-metastore>`_ for more details.

    .. note::

        Most of Hadoop instances use Kerberos authentication. In this case, you should call ``kinit``
        before starting Spark session to generate Kerberos ticket. See :ref:`kerberos-install`.

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

        spark = SparkSession.builder.appName("spark-app-name").enableHiveSupport().getOrCreate()

        hive = Hive(cluster="rnd-dwh", spark=spark)

    Hive connection initialization with Kerberos support

    .. code:: python

        from onetl.connection import Hive
        from pyspark.sql import SparkSession

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

        hive = Hive(cluster="rnd-dwh", spark=spark)
    """

    class WriteOptions(GenericOptions):
        """Hive source writing options.

        You can pass here key-value items which then will be converted to calls
        of :obj:`pyspark.sql.readwriter.DataFrameWriter` methods.

        For example, ``Hive.WriteOptions(mode="append", partitionBy="reg_id")`` will
        be converted to ``df.write.mode("append").partitionBy("reg_id")`` call, and so on.

        .. note::

            You can pass any method and its value
            `supported by Spark <https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html>`_,
            even if it is not mentioned in this documentation. **Its name should be in** ``camelCase``!

            The set of supported options depends on Spark version used.

        Examples
        --------

        Writing options initialization

        .. code:: python

            options = Hive.WriteOptions(mode="append", partitionBy="reg_id", someNewOption="value")
        """

        class Config:
            known_options: frozenset = frozenset()
            alias_generator = to_camel
            extra = "allow"

        mode: HiveWriteMode = HiveWriteMode.APPEND
        """Mode of writing data into target table.

        Possible values:
            * ``append`` (default)
                Appends data into existing partition/table, or create partition/table if it does not exist.

                Almost like Spark's ``insertInto(overwrite=False)``.

                Behavior in details:

                * Table does not exist
                    Table is created using other options from current class (``format``, ``compression``, etc).

                * Table exists, but not partitioned
                    Data is appended to a table. Table is still not partitioned (DDL is unchanged)

                * Table exists and partitioned, but partition is present only in dataframe
                    Partition is created based on table's ``PARTITIONED BY (...)`` options

                * Table exists and partitioned, partition is present in both dataframe and table
                    Data is appended to existing partition

                * Table exists and partitioned, but partition is present only in table
                    Existing partition is left intact

            * ``overwrite_partitions``
                Overwrites data in the existing partition, or create partition/table if it does not exist.

                Almost like Spark's ``insertInto(overwrite=True)`` +
                ``spark.sql.sources.partitionOverwriteMode=dynamic``.

                Behavior in details:

                * Table does not exist
                    Table is created using other options from current class (``format``, ``compression``, etc).

                * Table exists, but not partitioned
                    Data is **overwritten in all the table**. Table is still not partitioned (DDL is unchanged)

                * Table exists and partitioned, but partition is present only in dataframe
                    Partition is created based on table's ``PARTITIONED BY (...)`` options

                * Table exists and partitioned, partition is present in both dataframe and table
                    Data is **overwritten in existing partition**

                * Table exists and partitioned, but partition is present only in table
                    Existing partition is left intact

            * ``overwrite_table``
                **Recreates table** (via ``DROP + CREATE``), **overwriting all existing data**.
                **All existing partitions are dropped.**

                Same as Spark's ``saveAsTable(mode=overwrite)`` +
                ``spark.sql.sources.partitionOverwriteMode=static`` (NOT ``insertInto``)!

                .. warning::

                    Table is recreated using other options from current class (``format``, ``compression``, etc)
                    **instead of using original table options**. Be careful

        .. note::

            ``error`` and ``ignore`` modes are not supported.

        .. note::

            Unlike Spark, config option ``spark.sql.sources.partitionOverwriteMode``
            does not affect behavior of any ``mode``
        """

        format: str = "orc"
        """Format of files which should be used for storing table data.

        Examples: ``orc`` (default), ``parquet``, ``csv`` (NOT recommended)

        .. note::

            It's better to use column-based formats like ``orc`` or ``parquet``,
            not row-based (``csv``, ``json``)

        .. warning::

            Used **only** while **creating new table**, or if ``mode=overwrite_table``
        """

        partition_by: Optional[Union[List[str], str]] = None
        """
        List of columns should be used for data partitioning. ``None`` means partitioning is disabled.

        Each partition is a folder in HDFS which contains only files with the specific column value,
        like ``myschema.db/mytable/col1=value1``, ``myschema.db/mytable/col1=value2``, and so on.

        Multiple partitions columns means nested folder structure, like ``col1=val1/col2/val``.

        If ``WHERE`` clause in the query contains expression like ``partition = value``,
        Hive automatically filters up only specific partition.

        Examples: ``reg_id`` or ``["reg_id", "business_dt"]``

        .. note::

            Values should be scalars (integers, strings),
            and either static (``countryId``) or incrementing (dates, years), with low
            number of distinct values.

            Columns like ``userId`` or ``datetime``/``timestamp`` cannot be used for partitioning.

        .. warning::

            Used **only** while **creating new table**, or if ``mode=overwrite_table``
        """

        bucket_by: Optional[Tuple[int, Union[List[str], str]]] = None  # noqa: WPS234
        """Number of buckets plus bucketing columns. ``None`` means bucketing is disabled.

        Each bucket is created as a set of files with name containing ``hash(columns) mod num_buckets``,
        and used to remove shuffle from queries containing ``GROUP BY`` or ``JOIN`` over bucketing column,
        and to reduce number of files read by query containing ``=`` and ``IN`` predicates
        on bucketing column.

        Examples: ``(10, "user_id")``, ``(10, ["user_id", "user_phone"])``

        .. note::

            Bucketing should be used on columns containing values which have a lot of unique values,
            like ``userId``.

            Columns like ``countryId`` or ``date`` cannot be used for bucketing
            because of too low number of unique values.

        .. warning::

            It is recommended to use this option **ONLY** if you have a large table
            (hundreds of Gb or more), which is used mostly for JOINs with other tables,
            and you're inserting data using ``mode=overwrite_partitions``.

            Otherwise Spark will create a lot of small files
            (one file for each bucket and each executor), drastically decreasing HDFS performance.

        .. warning::

            Used **only** while **creating new table**, or if ``mode=overwrite_table``
        """

        sort_by: Optional[Union[List[str], str]] = None
        """Each file in a bucket will be sorted by these columns value. ``None`` means sorting is disabled.

        Examples: ``user_id`` or ``["user_id", "user_phone"]``

        .. note::

            Sorting columns should contain values which are used in ``ORDER BY`` clauses.

        .. warning::

            Could be used only with :obj:`bucket_by` option

        .. warning::

            Used **only** while **creating new table**, or if ``mode=overwrite_table``
        """

        compression: Optional[str] = None
        """Compressing algorithm which should be used for compressing created files in HDFS.
        ``None`` means compression is disabled.

        Examples: ``snappy``, ``zlib``

        .. warning::

            Used **only** while **creating new table**, or if ``mode=overwrite_table``
        """

        @validator("sort_by")
        def sort_by_cannot_be_used_without_bucket_by(cls, sort_by, values):
            options = values.copy()
            bucket_by = options.pop("bucket_by", None)
            if sort_by and not bucket_by:
                raise ValueError("`sort_by` option can only be used with non-empty `bucket_by`")

            return sort_by

        @root_validator
        def partition_overwrite_mode_is_not_allowed(cls, values):
            if values.get("partitionOverwriteMode") or values.get("partition_overwrite_mode"):
                raise ValueError(
                    "`partitionOverwriteMode` option should be replaced "
                    "with mode='overwrite_partitions' or 'overwrite_table'",
                )

            if values.get("insert_into") is not None or values.get("insertInto") is not None:
                raise ValueError(
                    "`insertInto` option was removed in onETL 0.4.0, "
                    "now df.write.insertInto or df.write.saveAsTable is selected based on table existence",
                )

            return values

    @deprecated(
        version="0.5.0",
        reason="Please use 'WriteOptions' class instead. Will be removed in v1.0.0",
        action="always",
    )
    class Options(WriteOptions):
        pass

    @support_hooks
    class slots:  # noqa: N801
        """:ref:`Slots <slot-decorator>` that could be implemented by third-party plugins."""

        @slot
        @staticmethod
        def normalize_cluster_name(cluster: str) -> str | None:
            """
            Normalize cluster name passed into Hive constructor. |support_hooks|

            If hooks didn't return anything, cluster name is left intact.

            Parameters
            ----------
            cluster : :obj:`str`
                Cluster name (raw)

            Returns
            -------
            str | None
                Normalized cluster name.

                If hook cannot be applied to a specific cluster, it should return ``None``.

            Examples
            --------

            .. code:: python

                from onetl.connection import Hive
                from onetl.hooks import hook


                @Hive.slots.normalize_cluster_name.bind
                @hook
                def normalize_cluster_name(cluster: str) -> str:
                    return cluster.lower()
            """

        @slot
        @staticmethod
        def get_known_clusters() -> set[str] | None:
            """
            Return collection of known clusters. |support_hooks|

            Cluster passed into Hive constructor should be present in this list.
            If hooks didn't return anything, no validation will be performed.

            Returns
            -------
            set[str] | None
                Collection of cluster names (normalized).

                If hook cannot be applied, it should return ``None``.

            Examples
            --------

            .. code:: python

                from onetl.connection import Hive
                from onetl.hooks import hook


                @Hive.slots.get_known_clusters.bind
                @hook
                def get_known_clusters() -> str[str]:
                    return {"rnd-dwh", "rnd-prod"}
            """

        @slot
        @staticmethod
        def get_current_cluster() -> str | None:
            """
            Get current cluster name. |support_hooks|

            Used in :obj:`~check` method to verify that connection is created only from the same cluster.
            If hooks didn't return anything, no validation will be performed.

            Returns
            -------
            str | None
                Current cluster name (normalized).

                If hook cannot be applied, it should return ``None``.

            Examples
            --------

            .. code:: python

                from onetl.connection import Hive
                from onetl.hooks import hook


                @Hive.slots.get_current_cluster.bind
                @hook
                def get_current_cluster() -> str:
                    # some magic here
                    return "rnd-dwh"
            """

    class Dialect(  # noqa: WPS215
        SupportTableWithDBSchema,
        SupportColumnsList,
        SupportDfSchemaNone,
        SupportWhereStr,
        SupportHintStr,
        SupportHWMExpressionStr,
        DBConnection.Dialect,
    ):
        pass

    cluster: Cluster

    @validator("cluster")
    def validate_cluster_name(cls, cluster):
        log.debug("|%s| Normalizing cluster %r name ...", cls.__name__, cluster)
        validated_cluster = cls.slots.normalize_cluster_name(cluster) or cluster
        if validated_cluster != cluster:
            log.debug("|%s|   Got %r", cls.__name__)

        log.debug("|%s| Checking if cluster %r is a known cluster ...", cls.__name__, validated_cluster)
        known_clusters = cls.slots.get_known_clusters()
        if known_clusters and validated_cluster not in known_clusters:
            raise ValueError(
                f"Cluster {validated_cluster!r} is not in the known clusters list: {sorted(known_clusters)!r}",
            )

        return validated_cluster

    @slot
    @classmethod
    def get_current(cls, spark: SparkSession):
        """
        Create connection for current cluster. |support_hooks|

        .. note::

            Can be used only if there are some hooks bound :obj:`~slots.get_current_cluster` slot.

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
        current_cluster = cls.slots.get_current_cluster()
        if not current_cluster:
            raise RuntimeError(
                f"{cls.__name__}.get_current() can be used only if there are "
                f"some hooks bound to {cls.__name__}.slots.get_current_cluster",
            )

        log.info("|%s| Got %r", cls.__name__, current_cluster)
        return cls(cluster=current_cluster, spark=spark)

    @property
    def instance_url(self) -> str:
        return self.cluster

    @slot
    def check(self):
        log.debug("|%s| Detecting current cluster...", self.__class__.__name__)
        current_cluster = self.slots.get_current_cluster()
        if current_cluster and self.cluster != current_cluster:
            raise ValueError("You can connect to a Hive cluster only from the same cluster")

        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()

        log.debug("|%s| Executing SQL query:", self.__class__.__name__)
        log_lines(self._check_query, level=logging.DEBUG)

        try:
            self._execute_sql(self._check_query)
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
        log_lines(query)

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
        log_lines(statement)

        self._execute_sql(statement).collect()
        log.info("|%s| Call succeeded", self.__class__.__name__)

    @slot
    def write_df(
        self,
        df: DataFrame,
        target: str,
        options: WriteOptions | dict | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)

        try:
            self.get_df_schema(target)
            table_exists = True

            log.info("|%s| Table %r already exists", self.__class__.__name__, target)
        except Exception:
            table_exists = False

        if table_exists and write_options.mode != HiveWriteMode.OVERWRITE_TABLE:
            # using saveAsTable on existing table does not handle
            # spark.sql.sources.partitionOverwriteMode=dynamic, so using insertInto instead.
            self._insert_into(df, target, options)
        else:
            # if someone needs to recreate the entire table using new set of options, like partitionBy or bucketBy,
            # mode="overwrite_table" should be used
            self._save_as_table(df, target, options)

    @slot
    def read_df(
        self,
        source: str,
        columns: list[str] | None = None,
        hint: str | None = None,
        where: str | None = None,
        df_schema: StructType | None = None,
        start_from: Statement | None = None,
        end_at: Statement | None = None,
    ) -> DataFrame:
        where = self.Dialect._condition_assembler(condition=where, start_from=start_from, end_at=end_at)
        sql_text = get_sql_query(
            table=source,
            columns=columns,
            where=where,
            hint=hint,
        )

        return self.sql(sql_text)

    @slot
    def get_df_schema(
        self,
        source: str,
        columns: list[str] | None = None,
    ) -> StructType:
        log.info("|%s| Fetching schema of table table %r", self.__class__.__name__, source)
        query = get_sql_query(source, columns=columns, where="1=0", compact=True)

        log.debug("|%s| Executing SQL query:", self.__class__.__name__)
        log_lines(query, level=logging.DEBUG)

        df = self._execute_sql(query)
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
    ) -> Tuple[Any, Any]:
        log.info("|Spark| Getting min and max values for column %r", column)

        sql_text = get_sql_query(
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
            hint=hint,
        )

        log.debug("|%s| Executing SQL query:", self.__class__.__name__)
        log_lines(sql_text, level=logging.DEBUG)

        df = self._execute_sql(sql_text)
        row = df.collect()[0]
        min_value = row["min"]
        max_value = row["max"]

        log.info("|Spark| Received values:")
        log_with_indent("MIN(%s) = %r", column, min_value)
        log_with_indent("MAX(%s) = %r", column, max_value)

        return min_value, max_value

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
        options: WriteOptions | dict | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)

        log.info("|%s| Inserting data into existing table %r", self.__class__.__name__, table)

        unsupported_options = write_options.dict(by_alias=True, exclude_unset=True, exclude={"mode"})
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

        overwrite_mode: str | None
        if write_options.mode == HiveWriteMode.OVERWRITE_PARTITIONS:
            overwrite_mode = "dynamic"
        elif write_options.mode == HiveWriteMode.OVERWRITE_TABLE:
            overwrite_mode = "static"
        else:
            overwrite_mode = None

        # Writer option "partitionOverwriteMode" was added to Spark only in 2.4.0
        # so using a workaround with patching Spark config and then setting up the previous value
        original_partition_overwrite_mode = self.spark.conf.get(PARTITION_OVERWRITE_MODE_PARAM, None)

        try:  # noqa: WPS501
            if overwrite_mode:
                self.spark.conf.set(PARTITION_OVERWRITE_MODE_PARAM, overwrite_mode)

            writer.insertInto(table, overwrite=bool(overwrite_mode))
        finally:
            self.spark.conf.unset(PARTITION_OVERWRITE_MODE_PARAM)
            if original_partition_overwrite_mode:
                self.spark.conf.set(PARTITION_OVERWRITE_MODE_PARAM, original_partition_overwrite_mode)

        log.info("|%s| Data is successfully inserted into table %r", self.__class__.__name__, table)

    def _save_as_table(
        self,
        df: DataFrame,
        table: str,
        options: WriteOptions | dict | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)

        log.info("|%s| Saving data to a table %r", self.__class__.__name__, table)

        writer = df.write
        for method, value in write_options.dict(by_alias=True, exclude_none=True, exclude={"mode"}).items():
            # <value> is the arguments that will be passed to the <method>
            # format orc, parquet methods and format simultaneously
            if hasattr(writer, method):
                if isinstance(value, Iterable) and not isinstance(value, str):
                    writer = getattr(writer, method)(*value)  # noqa: WPS220
                else:
                    writer = getattr(writer, method)(value)  # noqa: WPS220
            else:
                writer = writer.option(method, value)

        overwrite = write_options.mode != HiveWriteMode.APPEND
        writer.mode("overwrite" if overwrite else "append").saveAsTable(table)

        log.info("|%s| Table %r is successfully created", self.__class__.__name__, table)
