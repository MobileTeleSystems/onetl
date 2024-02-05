# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from logging import getLogger
from typing import TYPE_CHECKING, Optional

from pydantic import Field, PrivateAttr, validator

from onetl.base import BaseDBConnection
from onetl.hooks import slot, support_hooks
from onetl.impl import FrozenModel, GenericOptions
from onetl.log import (
    entity_boundary_log,
    log_dataframe_schema,
    log_options,
    log_with_indent,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

log = getLogger(__name__)


@support_hooks
class DBWriter(FrozenModel):
    """Class specifies schema and table where you can write your dataframe. |support_hooks|

    Parameters
    ----------
    connection : :obj:`onetl.connection.DBConnection`
        Class which contains DB connection properties. See :ref:`db-connections` section.

    target : str
        Table/collection/etc name to write data to.

        If connection has schema support, you need to specify the full name of the source
        including the schema, e.g. ``schema.name``.

    options : dict, :obj:`onetl.connection.DBConnection.WriteOptions`, default: ``None``
        Spark write options.

        For example:
        ``{"if_exists": "replace_entire_table", "compression": "snappy"}``
        or
        ``Hive.WriteOptions(if_exists="replace_entire_table", compression="snappy")``

        .. note::

            Some sources does not support writing options.


    Examples
    --------
    Simple Writer creation:

    .. code:: python

        from onetl.connection import Postgres
        from onetl.db import DBWriter
        from pyspark.sql import SparkSession

        maven_packages = Postgres.get_packages()
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        postgres = Postgres(
            host="postgres.domain.com",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )

        writer = DBWriter(
            connection=postgres,
            target="fiddle.dummy",
        )

    Writer creation with options:

    .. code:: python

        from onetl.connection import Postgres
        from onetl.db import DBWriter
        from pyspark.sql import SparkSession

        maven_packages = Postgres.get_packages()
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        postgres = Postgres(
            host="postgres.domain.com",
            user="your_user",
            password="***",
            database="target_db",
            spark=spark,
        )

        options = {"truncate": "true", "batchsize": 1000}
        # or (it is the same):
        options = Postgres.WriteOptions(truncate=True, batchsize=1000)

        writer = DBWriter(
            connection=postgres,
            target="fiddle.dummy",
            options=options,
        )

    Writer to Hive with options:

    .. code:: python

        from onetl.db import DBWriter
        from onetl.connection import Hive
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("spark-app-name").enableHiveSupport().getOrCreate()

        hive = Hive(cluster="rnd-dwh", spark=spark)

        options = {"compression": "snappy", "partitionBy": "id"}
        # or (it is the same):
        options = Hive.WriteOptions(compression="snappy", partitionBy="id")

        writer = DBWriter(
            connection=hive,
            target="default.test",
            options=options,
        )
    """

    connection: BaseDBConnection
    target: str = Field(alias="table")
    options: Optional[GenericOptions] = None

    _connection_checked: bool = PrivateAttr(default=False)

    @validator("target", pre=True, always=True)
    def validate_target(cls, target, values):
        connection: BaseDBConnection = values["connection"]
        return connection.dialect.validate_name(target)

    @validator("options", pre=True, always=True)
    def validate_options(cls, options, values):
        connection = values.get("connection")
        write_options_class = getattr(connection, "WriteOptions", None)
        if write_options_class:
            return write_options_class.parse(options)

        if options:
            raise ValueError(
                f"{connection.__class__.__name__} does not implement WriteOptions, but {options!r} is passed",
            )

        return None

    @slot
    def run(self, df: DataFrame):
        """
        Method for writing your df to specified target. |support_hooks|

        .. note :: Method does support only **batching** DataFrames.

        Parameters
        ----------
        df : pyspark.sql.dataframe.DataFrame
            Spark dataframe

        Examples
        --------

        Write df to target:

        .. code:: python

            writer.run(df)
        """
        if df.isStreaming:
            raise ValueError(f"DataFrame is streaming. {self.__class__.__name__} supports only batch DataFrames.")

        entity_boundary_log(log, msg=f"{self.__class__.__name__}.run() starts")

        if not self._connection_checked:
            self._log_parameters()
            log_dataframe_schema(log, df)
            self.connection.check()
            self._connection_checked = True

        self.connection.write_df_to_target(
            df=df,
            target=str(self.target),
            **self._get_write_kwargs(),
        )

        entity_boundary_log(log, msg=f"{self.__class__.__name__}.run() ends", char="-")

    def _log_parameters(self) -> None:
        log.info("|Spark| -> |%s| Writing DataFrame to target using parameters:", self.connection.__class__.__name__)
        log_with_indent(log, "target = '%s'", self.target)

        options = self.options.dict(by_alias=True, exclude_none=True) if self.options else None
        log_options(log, options)

    def _get_write_kwargs(self) -> dict:
        if self.options:
            return {"options": self.options}

        return {}
