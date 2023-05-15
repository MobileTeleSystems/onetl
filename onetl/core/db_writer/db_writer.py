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

from logging import getLogger
from typing import TYPE_CHECKING, Optional

from etl_entities import Table
from pydantic import Field, validator

from onetl.base import BaseDBConnection
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


class DBWriter(FrozenModel):
    """Class specifies schema and table where you can write your dataframe.

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
        ``{"mode": "overwrite", "compression": "snappy"}``
        or
        ``Hive.WriteOptions(mode="overwrite", compression="snappy")``

        .. note::

            Some sources does not support writing options.


    Examples
    --------
    Simple Writer creation:

    .. code:: python

        from onetl.connection import Postgres
        from onetl.core import DBWriter
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", Postgres.package)
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
        from onetl.core import DBWriter
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", Postgres.package)
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

        from onetl.core import DBWriter
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
    target: Table = Field(alias="table")
    options: Optional[GenericOptions] = None

    @validator("target", pre=True, always=True)
    def validate_target(cls, target, values):
        connection: BaseDBConnection = values["connection"]
        dialect = connection.Dialect
        if isinstance(target, str):
            # target="dbschema.table" or target="table", If target="dbschema.some.table" in class Table will raise error.
            target = Table(name=target, instance=connection.instance_url)
            # Here Table(name='target', db='dbschema', instance='some_instance')
        return dialect.validate_name(connection, target)

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

    def run(self, df: DataFrame):
        """
        Method for writing your df to specified target.

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

        entity_boundary_log(msg="DBWriter starts")

        self._log_parameters()
        log_dataframe_schema(df)
        self.connection.check()
        self.connection.write_df(
            df=df,
            target=str(self.target),
            **self._get_write_kwargs(),
        )

        entity_boundary_log(msg="DBWriter ends", char="-")

    def _log_parameters(self) -> None:
        log.info("|Spark| -> |%s| Writing DataFrame to target using parameters:", self.connection.__class__.__name__)
        log_with_indent("target = '%s'", self.target)

        options = self.options.dict(by_alias=True, exclude_none=True) if self.options else None
        log_options(options)

    def _get_write_kwargs(self) -> dict:
        if self.options:
            return {"options": self.options}

        return {}
