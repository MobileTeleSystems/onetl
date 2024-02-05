# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import logging
import warnings
from typing import TYPE_CHECKING, Any
from urllib import parse as parser

from etl_entities.instance import Host
from pydantic import SecretStr, validator

from onetl._util.classproperty import classproperty
from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.spark import get_spark_version
from onetl._util.version import Version
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.mongodb.dialect import MongoDBDialect
from onetl.connection.db_connection.mongodb.options import (
    MongoDBCollectionExistBehavior,
    MongoDBPipelineOptions,
    MongoDBReadOptions,
    MongoDBWriteOptions,
)
from onetl.exception import MISSING_JVM_CLASS_MSG
from onetl.hooks import slot, support_hooks
from onetl.hwm import Window
from onetl.impl import GenericOptions
from onetl.log import log_dataframe_schema, log_json, log_options, log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = logging.getLogger(__name__)


class MongoDBExtra(GenericOptions):
    class Config:
        extra = "allow"


@support_hooks
class MongoDB(DBConnection):
    """MongoDB connection. |support_hooks|

    Based on package ``org.mongodb.spark:mongo-spark-connector``
    (`MongoDB connector for Spark <https://www.mongodb.com/docs/spark-connector/current/>`_)

    .. dropdown:: Version compatibility

        * MongoDB server versions: 4.0 or higher
        * Spark versions: 3.2.x - 3.4.x
        * Scala versions: 2.12 - 2.13
        * Java versions: 8 - 20

        See `official documentation <https://www.mongodb.com/docs/spark-connector/current/>`_.

    .. warning::

        To use MongoDB connector you should have PySpark installed (or injected to ``sys.path``)
        BEFORE creating the connector instance.

        You can install PySpark as follows:

        .. code:: bash

            pip install onetl[spark]  # latest PySpark version

            # or
            pip install onetl pyspark=3.4.2  # pass specific PySpark version

        See :ref:`install-spark` installation instruction for more details.

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

    extra : dict, default: ``None``
        Specifies one or more extra parameters by which clients can connect to the instance.

        For example: ``{"tls": "false"}``

        See `Connection string options documentation
        <https://www.mongodb.com/docs/manual/reference/connection-string/#std-label-connections-connection-options>`_
        for more details

    spark : :obj:`pyspark.sql.SparkSession`
        Spark session.

    Examples
    --------

    MongoDB connection initialization.

    .. code:: python

        from onetl.connection import MongoDB
        from pyspark.sql import SparkSession

        # Create Spark session with MongoDB connector loaded
        maven_packages = MongoDB.get_packages(spark_version="3.2")
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        # Create connection
        mongo = MongoDB(
            host="master.host.or.ip",
            user="user",
            password="*****",
            database="target_database",
            spark=spark,
        )
    """

    database: str
    host: Host
    user: str
    password: SecretStr
    port: int = 27017
    extra: MongoDBExtra = MongoDBExtra()

    Dialect = MongoDBDialect
    ReadOptions = MongoDBReadOptions
    WriteOptions = MongoDBWriteOptions
    PipelineOptions = MongoDBPipelineOptions
    Extra = MongoDBExtra

    @slot
    @classmethod
    def get_packages(
        cls,
        scala_version: str | Version | None = None,
        spark_version: str | Version | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        .. warning::

            You should pass at least one parameter.

        Parameters
        ----------
        scala_version : str, optional
            Scala version in format ``major.minor``.

            If ``None``, ``spark_version`` is used to determine Scala version.

        spark_version : str, optional
            Spark version in format ``major.minor``.

            Used only if ``scala_version=None``.

        Examples
        --------

        .. code:: python

            from onetl.connection import MongoDB

            MongoDB.get_packages(scala_version="2.11")
            MongoDB.get_packages(spark_version="3.2")

        """

        # Connector version is fixed, so we can perform checks for Scala/Spark version
        if scala_version:
            scala_ver = Version.parse(scala_version)
        elif spark_version:
            spark_ver = Version.parse(spark_version)
            if spark_ver.major < 3:
                raise ValueError(f"Spark version must be at least 3.0, got {spark_ver}")
            scala_ver = get_default_scala_version(spark_ver)
        else:
            raise ValueError("You should pass either `scala_version` or `spark_version`")

        if scala_ver.digits(2) < (2, 12) or scala_ver.digits(2) > (2, 13):
            raise ValueError(f"Scala version must be 2.12 - 2.13, got {scala_ver}")

        # https://mvnrepository.com/artifact/org.mongodb.spark/mongo-spark-connector
        return [f"org.mongodb.spark:mongo-spark-connector_{scala_ver.digits(2)}:10.1.1"]

    @classproperty
    def package_spark_3_2(cls) -> str:
        """Get package name to be downloaded by Spark 3.2."""
        msg = (
            "`MongoDB.package_spark_3_2` will be removed in 1.0.0, "
            "use `MongoDB.get_packages(spark_version='3.2')` instead"
        )
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"

    @classproperty
    def package_spark_3_3(cls) -> str:
        """Get package name to be downloaded by Spark 3.3."""
        msg = (
            "`MongoDB.package_spark_3_3` will be removed in 1.0.0, "
            "use `MongoDB.get_packages(spark_version='3.3')` instead"
        )
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"

    @classproperty
    def package_spark_3_4(cls) -> str:
        """Get package name to be downloaded by Spark 3.4."""
        msg = (
            "`MongoDB.package_spark_3_4` will be removed in 1.0.0, "
            "use `MongoDB.get_packages(spark_version='3.4')` instead"
        )
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"

    @slot
    def pipeline(
        self,
        collection: str,
        pipeline: dict | list[dict],
        df_schema: StructType | None = None,
        options: MongoDBPipelineOptions | dict | None = None,
    ):
        """
        Execute a pipeline for a specific collection, and return DataFrame. |support_hooks|

        Almost like `Aggregation pipeline syntax <https://www.mongodb.com/docs/manual/core/aggregation-pipeline/>`_
        in MongoDB:

        .. code:: js

            db.collection.aggregate([{"$match": ...}, {"$group": ...}])

        but pipeline is executed on Spark executors, in a distributed way.

        .. note::

            This method does not support :ref:`strategy`,
            use :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` instead

        .. note::

            Statement is executed in read-write connection,
            so if you're calling some stored functions, you can make changes
            to the data source.

            Unfortunately, Spark does no provide any option to change this behavior.

        Parameters
        ----------

        collection : str
            Collection name.

        pipeline : dict | list[dict]
            Pipeline containing a database query.
            See `Aggregation pipeline syntax <https://www.mongodb.com/docs/manual/core/aggregation-pipeline/>`_.

        df_schema : StructType, default: ``None``
            Schema describing the resulting DataFrame.

        options : PipelineOptions | dict, default:  ``None``
            Additional pipeline options, see :obj:`~PipelineOptions`.

        Examples
        --------

        Get document with a specific ``_id``:

        .. code:: python

            df = connection.pipeline(
                collection="collection_name",
                pipeline={"$match": {"_id": {"$eq": 1}}},
            )

        Calculate aggregation and get result:

        .. code:: python

            df = connection.pipeline(
                collection="collection_name",
                pipeline={
                    "$group": {
                        "_id": 1,
                        "min": {"$min": "$column_int"},
                        "max": {"$max": "$column_int"},
                    }
                },
            )

        Explicitly pass DataFrame schema:

        .. code:: python

            from pyspark.sql.types import (
                DoubleType,
                IntegerType,
                StringType,
                StructField,
                StructType,
                TimestampType,
            )

            df_schema = StructType(
                [
                    StructField("_id", IntegerType()),
                    StructField("some_string", StringType()),
                    StructField("some_int", IntegerType()),
                    StructField("some_datetime", TimestampType()),
                    StructField("some_float", DoubleType()),
                ],
            )

            df = connection.pipeline(
                collection="collection_name",
                df_schema=df_schema,
                pipeline={"$match": {"some_int": {"$gt": 999}}},
            )

        Pass additional options to pipeline execution:

        .. code:: python

            df = connection.pipeline(
                collection="collection_name",
                pipeline={"$match": {"_id": {"$eq": 1}}},
                options=MongoDB.PipelineOptions(hint={"_id": 1}),
            )

        """
        log.info("|%s| Executing aggregation pipeline:", self.__class__.__name__)

        read_options = self.PipelineOptions.parse(options).dict(by_alias=True, exclude_none=True)
        pipeline = self.dialect.prepare_pipeline(pipeline)
        log_with_indent(log, "collection = %r", collection)
        log_json(log, pipeline, name="pipeline")

        if df_schema:
            empty_df = self.spark.createDataFrame([], df_schema)
            log_dataframe_schema(log, empty_df)

        log_options(log, read_options)

        read_options["collection"] = collection
        read_options["aggregation.pipeline"] = json.dumps(pipeline)
        read_options["connection.uri"] = self.connection_url
        spark_reader = self.spark.read.format("mongodb").options(**read_options)

        if df_schema:
            spark_reader = spark_reader.schema(df_schema)

        return spark_reader.load()

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}/{self.database}"

    @slot
    def check(self):
        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()

        try:
            jvm = self.spark._jvm  # type: ignore
            client = jvm.com.mongodb.client.MongoClients.create(self.connection_url)
            list(client.listDatabaseNames().iterator())
            log.info("|%s| Connection is available.", self.__class__.__name__)
        except Exception as e:
            log.exception("|%s| Connection is unavailable", self.__class__.__name__)
            raise RuntimeError("Connection is unavailable") from e

        return self

    @slot
    def get_min_max_values(
        self,
        source: str,
        window: Window,
        hint: Any | None = None,
        where: Any | None = None,
        options: MongoDBReadOptions | dict | None = None,
    ) -> tuple[Any, Any]:
        log.info("|%s| Getting min and max values for column %r ...", self.__class__.__name__, window.expression)

        read_options = self.ReadOptions.parse(options).dict(by_alias=True, exclude_none=True)

        # The '_id' field must be specified in the request.
        pipeline: list[dict[str, Any]] = [
            {
                "$group": {
                    "_id": 1,
                    "min": {"$min": f"${window.expression}"},
                    "max": {"$max": f"${window.expression}"},
                },
            },
        ]
        final_where = self.dialect.apply_window(where, window)
        if final_where:
            pipeline.insert(0, {"$match": final_where})

        pipeline = self.dialect.prepare_pipeline(pipeline)

        log.info("|%s| Executing aggregation pipeline:", self.__class__.__name__)
        log_with_indent(log, "collection = %r", source)
        log_json(log, pipeline, "pipeline")
        log_json(log, hint, "hint")

        read_options["connection.uri"] = self.connection_url
        read_options["collection"] = source
        read_options["aggregation.pipeline"] = json.dumps(pipeline)
        if hint:
            read_options["hint"] = json.dumps(hint)

        df = self.spark.read.format("mongodb").options(**read_options).load()
        data = df.collect()
        if data:
            min_value = data[0]["min"]
            max_value = data[0]["max"]
        else:
            min_value = max_value = None

        log.info("|%s| Received values:", self.__class__.__name__)
        log_with_indent(log, "MIN(%s) = %r", window.expression, min_value)
        log_with_indent(log, "MAX(%s) = %r", window.expression, max_value)

        return min_value, max_value

    @slot
    def read_source_as_df(
        self,
        source: str,
        columns: list[str] | None = None,
        hint: dict | None = None,
        where: dict | None = None,
        df_schema: StructType | None = None,
        window: Window | None = None,
        limit: int | None = None,
        options: MongoDBReadOptions | dict | None = None,
    ) -> DataFrame:
        read_options = self.ReadOptions.parse(options).dict(by_alias=True, exclude_none=True)
        final_where = self.dialect.apply_window(where, window)
        pipeline = self.dialect.prepare_pipeline({"$match": final_where}) if final_where else None

        if pipeline:
            read_options["aggregation.pipeline"] = json.dumps(pipeline)

        if hint:
            read_options["hint"] = json.dumps(hint)

        read_options["connection.uri"] = self.connection_url
        read_options["collection"] = source

        log.info("|%s| Executing aggregation pipeline:", self.__class__.__name__)
        log_with_indent(log, "collection = %r", source)
        log_json(log, pipeline, "pipeline")
        log_json(log, hint, "hint")
        spark_reader = self.spark.read.format("mongodb").options(**read_options)

        if df_schema:
            spark_reader = spark_reader.schema(df_schema)

        df = spark_reader.load()

        if columns:
            df = df.select(*columns)

        if limit is not None:
            df = df.limit(limit)

        log.info("|Spark| DataFrame successfully created from SQL statement ")
        return df

    @slot
    def write_df_to_target(
        self,
        df: DataFrame,
        target: str,
        options: MongoDBWriteOptions | dict | None = None,
    ) -> None:
        write_options = self.WriteOptions.parse(options)
        write_options_dict = write_options.dict(by_alias=True, exclude_none=True, exclude={"if_exists"})
        write_options_dict["connection.uri"] = self.connection_url
        write_options_dict["collection"] = target
        mode = (
            "overwrite"
            if write_options.if_exists == MongoDBCollectionExistBehavior.REPLACE_ENTIRE_COLLECTION
            else "append"
        )

        if self._collection_exists(target):
            # MongoDB connector does not support mode=ignore and mode=error
            if write_options.if_exists == MongoDBCollectionExistBehavior.ERROR:
                raise ValueError("Operation stopped due to MongoDB.WriteOptions(if_exists='error')")
            elif write_options.if_exists == MongoDBCollectionExistBehavior.IGNORE:
                log.info(
                    "|%s| Skip writing to existing collection because of MongoDB.WriteOptions(if_exists='ignore')",
                    self.__class__.__name__,
                )
                return

        log.info("|%s| Saving data to a collection %r", self.__class__.__name__, target)
        df.write.format("mongodb").mode(mode).options(**write_options_dict).save()
        log.info("|%s| Collection %r is successfully written", self.__class__.__name__, target)

    @property
    def connection_url(self) -> str:
        prop = self.extra.dict(by_alias=True)
        parameters = "&".join(f"{k}={v}" for k, v in sorted(prop.items()))
        parameters = "?" + parameters if parameters else ""
        password = parser.quote(self.password.get_secret_value())
        return f"mongodb://{self.user}:{password}@{self.host}:{self.port}/{self.database}{parameters}"

    @validator("spark")
    def _check_java_class_imported(cls, spark):
        java_class = "com.mongodb.spark.sql.connector.MongoTableProvider"

        try:
            try_import_java_class(spark, java_class)
        except Exception as e:
            spark_version = get_spark_version(spark).digits(2)
            msg = MISSING_JVM_CLASS_MSG.format(
                java_class=java_class,
                package_source=cls.__name__,
                args=f"spark_version='{spark_version}'",
            )
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Missing Java class", exc_info=e, stack_info=True)
            raise ValueError(msg) from e
        return spark

    def _collection_exists(self, source: str) -> bool:
        jvm = self.spark._jvm
        client = jvm.com.mongodb.client.MongoClients.create(self.connection_url)  # type: ignore
        collections = set(client.getDatabase(self.database).listCollectionNames().iterator())
        if source in collections:
            log.info("|%s| Collection %r exists", self.__class__.__name__, source)
            return True
        log.info("|%s| Collection %r does not exist", self.__class__.__name__, source)
        return False
