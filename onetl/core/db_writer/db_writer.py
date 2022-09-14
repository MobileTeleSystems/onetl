from __future__ import annotations

import io
from contextlib import redirect_stdout
from enum import Enum
from logging import getLogger
from typing import TYPE_CHECKING, Optional

from etl_entities import Table
from pydantic import validator

from onetl.base import BaseDBConnection
from onetl.impl import FrozenModel, GenericOptions
from onetl.log import entity_boundary_log, log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

log = getLogger(__name__)


class DBWriter(FrozenModel):
    """Class specifies schema and table where you can write your dataframe.

    Parameters
    ----------
    connection : :obj:`onetl.connection.DBConnection`
        Class which contains DB connection properties. See :ref:`db-connections` section.

    table : str
        Schema and table which is read data from.

        You need to specify the full path to the table, like ``schema.table``

    options : dict, :obj:`onetl.connection.DBConnection.WriteOptions`, default: ``None``
        Spark write options.

        For example:
        ``{"mode": "overwrite", "compression": "snappy"}``
        or
        ``Hive.WriteOptions(mode="overwrite", compression="snappy")``


    Examples
    --------
    Simple Writer creation:

    .. code::

        from onetl.connection import Postgres
        from onetl.core import DBWriter
        from mtspark import get_spark

        spark = get_spark({
            "appName": "spark-app-name",
            "spark.jars.packages": [
                "default:skip",
                Postgres.package
            ],
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
            "spark.jars.packages": [
                "default:skip",
                Postgres.package
            ],
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
        options = Postgres.WriteOptions(truncate=True, batchsize=1000}

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
        options = Hive.WriteOptions(compression="snappy", partitionBy="id")

        writer = DBWriter(
            connection=hive,
            table="default.test",
            options=options,
        )
    """

    connection: BaseDBConnection
    table: Table
    options: Optional[GenericOptions] = None

    @validator("table", pre=True, always=True)
    def validate_table(cls, value, values):  # noqa: N805
        if isinstance(value, str):
            return Table(name=value, instance=values["connection"].instance_url)
        return value

    @validator("options", pre=True, always=True)
    def validate_options(cls, options, values):  # noqa: N805
        connection = values.get("connection")
        write_options_class = getattr(connection, "WriteOptions", None)
        if write_options_class:
            return write_options_class.parse(options)

        if options:
            raise ValueError(
                f"{connection.__class__.__name__} does not implement WriteOptions, but {options!r} is passed",
            )

        return None

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
        self.connection.save_df(  # type: ignore[call-arg]
            df=df,
            table=str(self.table),
            **self._get_write_kwargs(),
        )

        entity_boundary_log(msg="DBWriter ends", char="-")

    def _log_parameters(self) -> None:
        log.info(f"|Spark| -> |{self.connection.__class__.__name__}| Writing DataFrame to table using parameters:")
        log_with_indent(f"table = '{self.table}'")

        log_with_indent("")
        options = self.options and self.options.dict(by_alias=True, exclude_none=True)
        if options:
            log_with_indent("options:")
            for option, value in options.items():
                value_wrapped = f"'{value}'" if isinstance(value, Enum) else repr(value)
                log_with_indent(f"{option} = {value_wrapped}", indent=4)
        else:
            log_with_indent("options = None")
        log_with_indent("")

    def _log_dataframe_schema(self, df: DataFrame) -> None:
        log_with_indent("DataFrame schema")

        schema_tree = io.StringIO()
        with redirect_stdout(schema_tree):
            # unfortunately, printSchema immediately prints tree instead of returning it
            # so we need a hack
            df.printSchema()

        for line in schema_tree.getvalue().splitlines():
            log_with_indent(line, indent=4)

    def _get_write_kwargs(self) -> dict:
        if self.options:
            return {"options": self.options}

        return {}
