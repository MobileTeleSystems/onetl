#  Copyright 2022 MTS (Mobile Telesystems)
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
from typing import TYPE_CHECKING, Any, ClassVar
from urllib import parse as parser

from pydantic import SecretStr

if TYPE_CHECKING:
    from pyspark.sql.types import StructType
    from pyspark.sql import DataFrame

from onetl.connection.db_connection.db_connection import DBConnection
from onetl.impl import GenericOptions

log = logging.getLogger(__name__)


class MongoDBWriteMode(str, Enum):  # noqa: WPS600
    APPEND = "append"
    OVERWRITE = "overwrite"

    def __str__(self) -> str:
        return str(self.value)


PROHIBITED_OPTIONS = frozenset(
    (
        "uri",
        "database",
        "collection",
    ),
)

KNOWN_READ_OPTIONS = frozenset(
    (
        "localThreshold",
        "readPreference.name",
        "readPreference.tagSets",
        "readConcern.level",
        "sampleSize",
        "samplePoolSize",
        "partitioner",
        "partitionerOptions",
        "registerSQLHelperFunctions",
        "sql.inferschema.mapTypes.enabled",
        "sql.inferschema.mapTypes.minimumKeys",
        "sql.pipeline.includeNullFilters",
        "sql.pipeline.includeFiltersAndProjections",
        "pipeline",
        "hint",
        "collation",
        "allowDiskUse",
        "batchSize",
    ),
)

KNOWN_WRITE_OPTIONS = frozenset(
    (
        "extendedBsonTypes",
        "localThreshold",
        "replaceDocument",
        "maxBatchSize",
        "writeConcern.w",
        "writeConcern.journal",
        "writeConcern.wTimeoutMS",
        "shardKey",
        "forceInsert",
        "ordered",
    ),
)


class MongoDB(DBConnection):
    """MongoDB connection.

    Based on package ``org.mongodb.spark:mongo-spark-connector``
    (`MongoDB connector for Spark <https://www.mongodb.com/docs/spark-connector/master/python-api/>`_)

    .. warning::

        To use Greenplum connector you should have PySpark installed (or injected to ``sys.path``)
        BEFORE creating the connector instance.

        You can install PySpark as follows:

        .. code:: bash

            pip install onetl[spark]  # latest PySpark version

            # or
            pip install onetl pyspark=3.3.1  # pass specific PySpark version

        See :ref:`spark-install` instruction for more details.

    Parameters
    ----------
    host : str
        Host of MongoDB. For example: ``test.mongodb.com`` or ``193.168.1.17``.

    port : int, default: ``27017``.
        Port of MongoDB

    user : str
        User, which have proper access to the database. For example: ``some_user``.

    password : str
        Password for database connection.

    database : str
        Database in MongoDB.

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session.

    Examples
    --------

    MongoDB connection initialization.

    .. code:: python

        from onetl.connection import MongoDB
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", MongoDB.package_spark_2_4)
            .getOrCreate()
        )

        mongo = MongoDB(
            host="master.host.or.ip",
            user="user",
            password="*****",
            database="target_database",
            spark=spark,
        )
    """

    database: str
    host: str
    user: str
    password: SecretStr
    port: int = 27017
    package_spark_2_4: ClassVar[str] = "org.mongodb.spark:mongo-spark-connector_2.11:2.4.4"  # noqa: WPS114
    package_spark_2_3: ClassVar[str] = "org.mongodb.spark:mongo-spark-connector_2.11:2.3.6"  # noqa: WPS114
    package_spark_3_2: ClassVar[str] = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"  # noqa: WPS114
    package_spark_3_3: ClassVar[str] = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"  # noqa: WPS114

    class ReadOptions(GenericOptions):
        """MongoDB connector for Spark reading options.

        .. note ::

            You can pass any value
            `supported by connector <https://www.mongodb.com/docs/spark-connector/master/configuration/
            #:~:text=See%20Cache%20Configuration.-,Input%20Configuration,-You%20can%20configure>`_,
            even if it is not mentioned in this documentation.

            The set of supported options depends on connector version.

        .. warning::

            Options ``uri``, ``database``, ``collection`` are populated from connection attributes,
            and cannot be set in ``ReadOptions`` class.

        Examples
        --------

        Read options initialization

        .. code:: python

            MongoDB.ReadOptions(
                pipeline="{'$match': {'number':{'$gte': 3}}}",
            )
        """

        class Config:
            prohibited_options = PROHIBITED_OPTIONS
            known_options = KNOWN_READ_OPTIONS
            extra = "allow"

    class WriteOptions(GenericOptions):
        """MongoDB connector for writing reading options.

        .. note ::

            You can pass any value
            `supported by connector <https://www.mongodb.com/docs/spark-connector/master/configuration/
            #:~:text=input.database%3Dbar-,Output%20Configuration,-The%20following%20options>`_,
            even if it is not mentioned in this documentation.

            The set of supported options depends on connector version.

        .. warning::

            Options ``uri``, ``database``, ``collection`` are populated from connection attributes,
            and cannot be set in ``WriteOptions`` class.

        Examples
        --------

        Write options initialization

        .. code:: python

            options = MongoDB.WriteOptions(
                mode="append",
                sampleSize=500,
                localThreshold=20,
            )
        """

        mode: MongoDBWriteMode = MongoDBWriteMode.APPEND
        """Mode of writing data into target table.

        Possible values:
            * ``append`` (default)
                Appends data into existing table.

                Behavior in details:

                * Table does not exist
                    Table is created using other options from current class
                    (``shardkey`` and others).

                * Table exists
                    Data is appended to a table. Table has the same DDL as before writing data

            * ``overwrite``
                Overwrites data in the entire table (**table is dropped and then created, or truncated**).

                Behavior in details:

                * Table does not exist
                    Table is created using other options from current class
                    (``shardkey`` and others).

                * Table exists
                    Table content is replaced with dataframe content.

                    After writing completed, target table could either have the same DDL as
                    before writing data, or can be recreated.

        .. note::

            ``error`` and ``ignore`` modes are not supported.
        """

        class Config:
            prohibited_options = PROHIBITED_OPTIONS
            known_options = KNOWN_WRITE_OPTIONS
            extra = "allow"

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}/{self.database}"

    def check(self):
        pass  # noqa: WPS420

    def get_schema(self, table: str, columns: list[str] | None = None) -> StructType:  # noqa: WPS463
        pass  # noqa: WPS420

    def get_min_max_bounds(  # noqa: WPS463
        self,
        table: str,
        column: str,
        expression: str | None = None,
        hint: str | None = None,
        where: str | None = None,
    ) -> tuple[Any, Any]:
        return ""  # type: ignore

    @property
    def url(self) -> str:
        return f"mongodb://{self.user}:{parser.quote(self.password.get_secret_value())}@{self.host}:{self.port}"

    def read_table(
        self,
        table: str,
        columns: list[str] | None = None,
        hint: str | None = None,
        where: str | None = None,
        options: ReadOptions | dict | None = None,
    ) -> DataFrame:
        read_options = self.ReadOptions.parse(options).dict(by_alias=True, exclude_none=True)

        if where:
            read_options["pipeline"] = where

        if hint:
            read_options["hint"] = hint

        read_options["spark.mongodb.input.database"] = self.database
        read_options["spark.mongodb.input.collection"] = table
        read_options["spark.mongodb.input.uri"] = self.url

        df = self.spark.read.format("mongo").options(**read_options).load()

        if columns:
            return df.select(*columns)

        return df

    def save_df(
        self,
        df: DataFrame,
        table: str,
        options: WriteOptions | dict | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)
        mode = write_options.mode
        write_options = write_options.dict(by_alias=True, exclude_none=True, exclude={"mode"})
        write_options["uri"] = self.url
        write_options["spark.mongodb.output.database"] = self.database
        write_options["spark.mongodb.output.collection"] = table
        df.write.format("mongo").mode(mode).options(**write_options).save()
