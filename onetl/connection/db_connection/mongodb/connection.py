# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import logging
import warnings
from typing import TYPE_CHECKING, Any, ClassVar, Optional
from urllib import parse as parser

from etl_entities.instance import Host

try:
    from pydantic.v1 import PrivateAttr, SecretStr, validator
except (ImportError, AttributeError):
    from pydantic import PrivateAttr, SecretStr, validator  # type: ignore[no-redef, assignment]

from onetl._util.classproperty import classproperty
from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.spark import (
    get_client_info,
    get_spark_version,
    override_job_description,
)
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

    Based on package `org.mongodb.spark:mongo-spark-connector:10.4.1 <https://mvnrepository.com/artifact/org.mongodb.spark/mongo-spark-connector_2.12/10.4.1>`_
    (`MongoDB connector for Spark <https://www.mongodb.com/docs/spark-connector/current/>`_)

    .. seealso::

        Before using this connector please take into account :ref:`mongodb-prerequisites`

    .. versionadded:: 0.7.0

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
        maven_packages = MongoDB.get_packages(spark_version="3.4")
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
        ).check()
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

    _server_version: Optional[Version] = PrivateAttr(default=None)

    # any small collection with always present in db, and which any user can access
    # https://www.mongodb.com/docs/manual/reference/system-collections/
    _CHECK_DUMMY_COLLECTION: ClassVar[str] = "admin.system.version"

    @slot
    @classmethod
    def get_packages(
        cls,
        scala_version: str | None = None,
        spark_version: str | None = None,
        package_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. Allows specifying custom MongoDB Spark connector versions. |support_hooks|

        .. versionadded:: 0.9.0

        Parameters
        ----------
        scala_version : str, optional
            Scala version in format ``major.minor``.

            If ``None``, ``spark_version`` is used to determine Scala version.

        spark_version : str, optional
            Spark version in format ``major.minor``. Used only if ``scala_version=None``.

        package_version : str, optional
            Specifies the version of the MongoDB Spark connector to use. Defaults to ``10.4.1``.

            .. versionadded:: 0.11.0

        Examples
        --------
        .. code:: python

            from onetl.connection import MongoDB

            MongoDB.get_packages(scala_version="2.12")

            # specify custom connector version
            MongoDB.get_packages(scala_version="2.12", package_version="10.4.1")
        """

        default_package_version = "10.4.1"

        if scala_version:
            scala_ver = Version(scala_version).min_digits(2)
        elif spark_version:
            spark_ver = Version(spark_version)
            if spark_ver.major < 3:
                raise ValueError(f"Spark version must be at least 3.0, got {spark_ver}")
            scala_ver = get_default_scala_version(spark_ver)
        else:
            raise ValueError("You should pass either `scala_version` or `spark_version`")

        connector_ver = Version(package_version or default_package_version).min_digits(2)

        if scala_ver < Version("2.12"):
            raise ValueError(f"Scala version must be at least 2.12, got {scala_ver}")

        return [f"org.mongodb.spark:mongo-spark-connector_{scala_ver.format('{0}.{1}')}:{connector_ver}"]

    @classproperty
    def package_spark_3_2(cls) -> str:
        """Get package name to be downloaded by Spark 3.2."""
        msg = (
            "`MongoDB.package_spark_3_2` will be removed in 1.0.0, "
            "use `MongoDB.get_packages(spark_version='3.2')` instead"
        )
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1"

    @classproperty
    def package_spark_3_3(cls) -> str:
        """Get package name to be downloaded by Spark 3.3."""
        msg = (
            "`MongoDB.package_spark_3_3` will be removed in 1.0.0, "
            "use `MongoDB.get_packages(spark_version='3.3')` instead"
        )
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1"

    @classproperty
    def package_spark_3_4(cls) -> str:
        """Get package name to be downloaded by Spark 3.4."""
        msg = (
            "`MongoDB.package_spark_3_4` will be removed in 1.0.0, "
            "use `MongoDB.get_packages(spark_version='3.4')` instead"
        )
        warnings.warn(msg, UserWarning, stacklevel=3)
        return "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1"

    @slot
    def pipeline(
        self,
        collection: str,
        pipeline: dict | list[dict] | None = None,
        df_schema: StructType | None = None,
        options: MongoDBPipelineOptions | dict | None = None,
    ):
        """
        Execute a pipeline for a specific collection, and return DataFrame. |support_hooks|

        Almost like `Aggregation pipeline syntax <https://www.mongodb.com/docs/manual/core/aggregation-pipeline/>`_
        in MongoDB:

        .. code:: js

            db.collection_name.aggregate([{"$match": ...}, {"$group": ...}])

        but pipeline is executed on Spark executors, in a distributed way.

        .. note::

            This method does not support :ref:`strategy`,
            use :obj:`DBReader <onetl.db.db_reader.db_reader.DBReader>` instead

        .. versionadded:: 0.7.0

        Parameters
        ----------

        collection : str
            Collection name.

        pipeline : dict | list[dict], optional
            Pipeline containing a database query.
            See `Aggregation pipeline syntax <https://www.mongodb.com/docs/manual/core/aggregation-pipeline/>`_.

        df_schema : StructType, optional
            Schema describing the resulting DataFrame.

        options : PipelineOptions | dict, optional
            Additional pipeline options, see :obj:`MongoDB.PipelineOptions <onetl.connection.db_connection.mongodb.options.MongoDBPipelineOptions>`.

        Examples
        --------

        Get document with a specific ``field`` value:

        .. code:: python

            df = connection.pipeline(
                collection="collection_name",
                pipeline={"$match": {"field": {"$eq": 1}}},
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
                    StructField("_id", StringType()),
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
                pipeline={"$match": {"field": {"$eq": 1}}},
                options=MongoDB.PipelineOptions(hint={"field": 1}),
            )

        """
        log.info("|%s| Executing aggregation pipeline:", self.__class__.__name__)

        read_options = self.PipelineOptions.parse(options).dict(by_alias=True, exclude_none=True)
        if pipeline:
            pipeline = self.dialect.prepare_pipeline(pipeline)

        log_with_indent(log, "collection = %r", collection)
        log_json(log, pipeline, name="pipeline")

        if df_schema:
            empty_df = self.spark.createDataFrame([], df_schema)
            log_dataframe_schema(log, empty_df)

        log_options(log, read_options)

        # exclude from the log
        read_options.update(self._get_connection_params(collection))
        if pipeline:
            read_options["aggregation.pipeline"] = json.dumps(pipeline)

        with override_job_description(self.spark, f"{self}.pipeline()"):
            spark_reader = self.spark.read.format("mongodb").options(**read_options)

            if df_schema:
                spark_reader = spark_reader.schema(df_schema)

            return spark_reader.load()

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}/{self.database}"

    def __str__(self):
        return f"{self.__class__.__name__}[{self.host}:{self.port}/{self.database}]"

    @slot
    def check(self):
        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()

        try:
            jvm = self.spark._jvm  # type: ignore
            client = jvm.com.mongodb.client.MongoClients.create(self.connection_url)
            list(client.listDatabaseNames().iterator())

            with override_job_description(self.spark, f"{self}.check()"):
                read_options = self._get_connection_params(self._CHECK_DUMMY_COLLECTION)
                self.spark.read.format("mongodb").options(**read_options).load().take(1)

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
        read_options.update(self._get_connection_params(source))

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
        read_options.update(self._get_connection_params(source))

        final_where = self.dialect.apply_window(where, window)
        pipeline = self.dialect.prepare_pipeline({"$match": final_where}) if final_where else None
        if pipeline:
            read_options["aggregation.pipeline"] = json.dumps(pipeline)

        if hint:
            read_options["hint"] = json.dumps(hint)

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
        write_options_dict.update(self._get_connection_params(target))
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
        params = self.extra.dict(by_alias=True)
        sorted_params = [(k, v) for k, v in sorted(params.items(), key=lambda x: x[0].lower())]
        query = parser.urlencode(sorted_params, quote_via=parser.quote)

        password = parser.quote(self.password.get_secret_value())
        parsed_url = parser.urlparse(f"mongodb://{self.user}:{password}@{self.host}:{self.port}/")
        # do not include /database as it be used as authSource
        return parser.urlunparse(parsed_url._replace(query=query))

    def _get_connection_params(self, collection: str):
        # https://www.mongodb.com/docs/spark-connector/current/batch-mode/batch-read-config/#std-label-spark-batch-read-conf
        result = {
            "connection.uri": self.connection_url,
            "database": self.database,
            "collection": collection,
        }

        if self._get_server_version() >= Version("4.4.0"):
            # According to:
            # https://mongodb.github.io/mongo-java-driver/5.0/apidocs/mongodb-driver-core/com/mongodb/client/model/BulkWriteOptions.html#comment(java.lang.String)
            result["comment"] = get_client_info(self.spark)

        return result

    @validator("spark")
    def _check_java_class_imported(cls, spark):
        java_class = "com.mongodb.spark.sql.connector.MongoTableProvider"

        try:
            try_import_java_class(spark, java_class)
        except Exception as e:
            spark_version = get_spark_version(spark).format("{major}.{minor}")
            msg = MISSING_JVM_CLASS_MSG.format(
                java_class=java_class,
                package_source=cls.__name__,
                args=f"spark_version='{spark_version}'",
            )
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Missing Java class", exc_info=e, stack_info=True)
            raise ValueError(msg) from e
        return spark

    def _get_server_version(self) -> Version:
        if self._server_version:
            return self._server_version

        jvm = self.spark._jvm  # type: ignore[attr-defined]
        client = jvm.com.mongodb.client.MongoClients.create(self.connection_url)  # type: ignore
        db = client.getDatabase(self.database)
        command = jvm.org.bson.BsonDocument("buildinfo", jvm.org.bson.BsonString(""))  # type: ignore
        self._server_version = Version(db.runCommand(command).get("version"))
        return self._server_version

    def _collection_exists(self, source: str) -> bool:
        jvm = self.spark._jvm  # type: ignore[attr-defined]
        client = jvm.com.mongodb.client.MongoClients.create(self.connection_url)  # type: ignore
        collections = set(client.getDatabase(self.database).listCollectionNames().iterator())
        if source in collections:
            log.info("|%s| Collection %r exists", self.__class__.__name__, source)
            return True
        log.info("|%s| Collection %r does not exist", self.__class__.__name__, source)
        return False
