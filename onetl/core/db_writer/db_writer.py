from __future__ import annotations

import io
from contextlib import redirect_stdout
from dataclasses import dataclass
from enum import Enum
from logging import getLogger
from typing import TYPE_CHECKING

from etl_entities import Table

from onetl.connection.db_connection import DBConnection
from onetl.log import entity_boundary_log, log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

log = getLogger(__name__)


@dataclass
class DBWriter:
    """Class specifies database and table where you can write your dataframe.

    Parameters
    ----------
    connection : :obj:`onetl.connection.DBConnection`
        Class which contains DB connection properties. See :ref:`db-connections` section.

    table : str
        Table which is read data from. You need to specify the full path to the table, including the schema.
        Like ``schema.name``

    options : dict, :obj:`onetl.connection.DBConnection.Options`, default: ``None``
        Spark JDBC or Hive write options.

        For example:
        ``{"mode": "overwrite", "compression": "snappy"}``
        or
        ``Hive.Options(mode="overwrite", compression="snappy")``


    Examples
    --------
    Simple Writer creation:

    .. code::

        from onetl.connection import Postgres
        from onetl.core import DBWriter
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": [Postgres.package],
        })

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )

        writer = DBWriter(
            connection=postgres,
            table="fiddle.dummy",
        )

    Writer creation with options:

    .. code::

        from onetl.connection import Postgres
        from onetl.core import DBWriter
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": [Postgres.package],
        })

        postgres = Postgres(
            host="test-db-vip.msk.mts.ru",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )

        options = {"truncate": "true", "batchsize": 1000}
        # or (it is the same):
        options = Postgres.Options(truncate=True, batchsize=1000}

        writer = DBWriter(
            connection=postgres,
            table="fiddle.dummy",
            options=options,
        )

    Writer to Hive with options:

    .. code::

        from onetl.core import DBWriter
        from onetl.connection import Hive
        from mtspark import get_spark

        spark = get_spark({"appName": "spark-app-name"})

        hive = Hive(spark=spark)

        options = {"compression": "snappy", "partitionBy": "id"}
        # or (it is the same):
        options = Hive.Options(compression="snappy", partitionBy="id")

        writer = DBWriter(
            connection=hive,
            table="default.test",
            options=options,
        )
    """

    connection: DBConnection
    table: Table
    options: DBConnection.Options

    def __init__(
        self,
        connection: DBConnection,
        table: str,
        options: DBConnection.Options | dict | None = None,
    ):
        self.connection = connection
        self.table = self._handle_table(table)
        self.options = self._handle_options(options)

    def run(self, df: DataFrame):
        """
        Method for writing your df to specified table.

        Parameters
        ----------
        df : pyspark.sql.dataframe.DataFrame
            Spark dataframe

        Examples
        --------

        Write df to table:

        .. code::

            writer.run(df)
        """

        entity_boundary_log(msg="DBWriter starts")

        self._log_parameters()
        self._log_dataframe_schema(df)

        self.connection.check()
        self.connection.save_df(
            df=df,
            table=str(self.table),
            options=self.options,
        )

        entity_boundary_log(msg="DBWriter ends", char="-")

    def _log_parameters(self) -> None:
        log.info(f"|Spark| -> |{self.connection.__class__.__name__}| Writing DataFrame to table using parameters:")
        log_with_indent(f"table = '{self.table}'")

        log.info("")
        log_with_indent("options:")
        for option, value in self.options.dict(exclude_none=True).items():
            value_wrapped = f"'{value}'" if isinstance(value, Enum) else repr(value)
            log_with_indent(f"    {option} = {value_wrapped}")
        log.info("")

    def _log_dataframe_schema(self, df: DataFrame) -> None:
        log_with_indent("DataFrame schema")

        schema_tree = io.StringIO()
        with redirect_stdout(schema_tree):
            # unfortunately, printSchema immediately prints tree instead of returning it
            # so we need a hack
            df.printSchema()

        for line in schema_tree.getvalue().splitlines():
            log_with_indent(f"    {line}")

    def _handle_table(self, table: str) -> Table:
        return Table(name=table, instance=self.connection.instance_url)

    def _handle_options(self, options: DBConnection.Options | dict | None) -> DBConnection.Options:
        return self.connection.to_options(options)
