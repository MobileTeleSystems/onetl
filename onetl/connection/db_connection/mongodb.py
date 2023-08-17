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

import json
import logging
import operator
import warnings
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Dict, Iterable, Mapping
from urllib import parse as parser

from etl_entities.instance import Host
from pydantic import Field, SecretStr, root_validator, validator

from onetl._util.classproperty import classproperty
from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.spark import get_spark_version
from onetl._util.version import Version
from onetl.base.base_db_connection import BaseDBConnection
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.dialect_mixins import (
    SupportColumnsNone,
    SupportDfSchemaStruct,
    SupportHWMColumnStr,
    SupportHWMExpressionNone,
    SupportTableWithoutDBSchema,
)
from onetl.exception import MISSING_JVM_CLASS_MSG
from onetl.hooks import slot, support_hooks
from onetl.hwm import Statement
from onetl.impl import GenericOptions
from onetl.log import log_dataframe_schema, log_json, log_options, log_with_indent

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = logging.getLogger(__name__)


_upper_level_operators = frozenset(  # noqa: WPS527
    [
        "$addFields",
        "$bucket",
        "$bucketAuto",
        "$changeStream",
        "$collStats",
        "$count",
        "$currentOp",
        "$densify",
        "$documents",
        "$facet",
        "$fill",
        "$geoNear",
        "$graphLookup",
        "$group",
        "$indexStats",
        "$limit",
        "$listLocalSessions",
        "$listSessions",
        "$lookup",
        "$merge",
        "$out",
        "$planCacheStats",
        "$project",
        "$redact",
        "$replaceRoot",
        "$replaceWith",
        "$sample",
        "$search",
        "$searchMeta",
        "$set",
        "$setWindowFields",
        "$shardedDataDistribution",
        "$skip",
        "$sort",
        "$sortByCount",
        "$unionWith",
        "$unset",
        "$unwind",
    ],
)


class MongoDBCollectionExistBehavior(str, Enum):
    APPEND = "append"
    REPLACE_ENTIRE_COLLECTION = "replace_entire_collection"

    def __str__(self) -> str:
        return str(self.value)

    @classmethod  # noqa: WPS120
    def _missing_(cls, value: object):  # noqa: WPS120
        if str(value) == "overwrite":
            warnings.warn(
                "Mode `overwrite` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `replace_entire_collection` instead",
                category=UserWarning,
                stacklevel=4,
            )
            return cls.REPLACE_ENTIRE_COLLECTION


PIPELINE_PROHIBITED_OPTIONS = frozenset(
    (
        "uri",
        "database",
        "collection",
        "pipeline",
    ),
)

PROHIBITED_OPTIONS = frozenset(
    (
        "uri",
        "database",
        "collection",
        "pipeline",
        "hint",
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


@support_hooks
class MongoDB(DBConnection):
    """MongoDB connection. |support_hooks|

    Based on package ``org.mongodb.spark:mongo-spark-connector``
    (`MongoDB connector for Spark <https://www.mongodb.com/docs/spark-connector/current/>`_)

    .. dropdown:: Version compatibility

        * MongoDB server versions: 4.0 or higher
        * Spark versions: 3.2.x - 3.4.x
        * Java versions: 8 - 20

        See `official documentation <https://www.mongodb.com/docs/spark-connector/current/>`_.

    .. warning::

        To use MongoDB connector you should have PySpark installed (or injected to ``sys.path``)
        BEFORE creating the connector instance.

        You can install PySpark as follows:

        .. code:: bash

            pip install onetl[spark]  # latest PySpark version

            # or
            pip install onetl pyspark=3.4.1  # pass specific PySpark version

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
        maven_packages = Greenplum.get_packages(spark_version="3.2")
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

    class Extra(GenericOptions):
        class Config:
            extra = "allow"

    database: str
    host: Host
    user: str
    password: SecretStr
    port: int = 27017
    extra: Extra = Extra()

    @slot
    @classmethod
    def get_packages(
        cls,
        scala_version: str | None = None,
        spark_version: str | None = None,
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

    class PipelineOptions(GenericOptions):
        """Aggregation pipeline options for MongoDB connector.

        The only difference from :obj:`~ReadOptions` that it is allowed to pass the 'hint' parameter.

        .. note ::

            You can pass any value
            `supported by connector <https://www.mongodb.com/docs/spark-connector/current/configuration/read>`_,
            even if it is not mentioned in this documentation.

            The set of supported options depends on connector version. See link above.

        .. warning::

            Options ``uri``, ``database``, ``collection``, ``pipeline`` are populated from connection attributes,
            and cannot be set in ``PipelineOptions`` class.

        Examples
        --------

        Pipeline options initialization

        .. code:: python

            MongoDB.PipelineOptions(
                hint="{'_id': 1}",
            )
        """

        class Config:
            prohibited_options = PIPELINE_PROHIBITED_OPTIONS
            known_options = KNOWN_READ_OPTIONS
            extra = "allow"

    class ReadOptions(GenericOptions):
        """Reading options for MongoDB connector.

        .. note ::

            You can pass any value
            `supported by connector <https://www.mongodb.com/docs/spark-connector/current/configuration/read>`_,
            even if it is not mentioned in this documentation.

            The set of supported options depends on connector version. See link above.

        .. warning::

            Options ``uri``, ``database``, ``collection``, ``pipeline``, ``hint`` are populated from connection
            attributes, and cannot be set in ``ReadOptions`` class.

        Examples
        --------

        Read options initialization

        .. code:: python

            MongoDB.ReadOptions(
                batchSize=10000,
            )
        """

        class Config:
            prohibited_options = PROHIBITED_OPTIONS
            known_options = KNOWN_READ_OPTIONS
            extra = "allow"

    class WriteOptions(GenericOptions):
        """Writing options for MongoDB connector.

        .. note ::

            You can pass any value
            `supported by connector <https://www.mongodb.com/docs/spark-connector/current/configuration/write/>`_,
            even if it is not mentioned in this documentation.

            The set of supported options depends on connector version. See link above.

        .. warning::

            Options ``uri``, ``database``, ``collection`` are populated from connection attributes,
            and cannot be set in ``WriteOptions`` class.

        Examples
        --------

        Write options initialization

        .. code:: python

            options = MongoDB.WriteOptions(
                if_exists="append",
                sampleSize=500,
                localThreshold=20,
            )
        """

        if_exists: MongoDBCollectionExistBehavior = Field(default=MongoDBCollectionExistBehavior.APPEND, alias="mode")
        """Behavior of writing data into existing collection.

        Possible values:
            * ``append`` (default)
                Adds new objects into existing collection.

                .. dropdown:: Behavior in details

                * Collection does not exist
                    Collection is created using options provided by user
                    (``shardkey`` and others).

                * Collection exists
                    Data is appended to a collection.

                    .. warning::

                        This mode does not check whether collection already contains
                        objects from dataframe, so duplicated objects can be created.

            * ``replace_entire_collection``
                **Collection is deleted and then created**.

                .. dropdown:: Behavior in details

                * Collection does not exist
                    Collection is created using options provided by user
                    (``shardkey`` and others).

                * Collection exists
                    Collection content is replaced with dataframe content.

        .. note::

            ``error`` and ``ignore`` modes are not supported.
        """

        class Config:
            prohibited_options = PROHIBITED_OPTIONS
            known_options = KNOWN_WRITE_OPTIONS
            extra = "allow"

        @root_validator(pre=True)
        def mode_is_deprecated(cls, values):
            if "mode" in values:
                warnings.warn(
                    "Option `MongoDB.WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                    "Use `MongoDB.WriteOptions(if_exists=...)` instead",
                    category=UserWarning,
                    stacklevel=3,
                )
            return values

    class Dialect(  # noqa: WPS215
        SupportTableWithoutDBSchema,
        SupportHWMExpressionNone,
        SupportColumnsNone,
        SupportDfSchemaStruct,
        SupportHWMColumnStr,
        DBConnection.Dialect,
    ):
        _compare_statements: ClassVar[Dict[Callable, str]] = {
            operator.ge: "$gte",
            operator.gt: "$gt",
            operator.le: "$lte",
            operator.lt: "$lt",
            operator.eq: "$eq",
            operator.ne: "$ne",
        }

        @classmethod
        def validate_where(
            cls,
            connection: BaseDBConnection,
            where: Any,
        ) -> dict | None:
            if where is None:
                return None

            if not isinstance(where, dict):
                raise ValueError(
                    f"{connection.__class__.__name__} requires 'where' parameter type to be 'dict', "
                    f"got {where.__class__.__name__!r}",
                )

            for key in where:
                cls._validate_top_level_keys_in_where_parameter(key)
            return where

        @classmethod
        def validate_hint(
            cls,
            connection: BaseDBConnection,
            hint: Any,
        ) -> dict | None:
            if hint is None:
                return None

            if not isinstance(hint, dict):
                raise ValueError(
                    f"{connection.__class__.__name__} requires 'hint' parameter type to be 'dict', "
                    f"got {hint.__class__.__name__!r}",
                )
            return hint

        @classmethod
        def prepare_pipeline(
            cls,
            pipeline: Any,
        ) -> Any:
            """
            Prepares pipeline (list or dict) to MongoDB syntax, but without converting it to string.
            """

            if isinstance(pipeline, datetime):
                return {"$date": pipeline.astimezone().isoformat()}

            if isinstance(pipeline, Mapping):
                return {cls.prepare_pipeline(key): cls.prepare_pipeline(value) for key, value in pipeline.items()}

            if isinstance(pipeline, Iterable) and not isinstance(pipeline, str):
                return [cls.prepare_pipeline(item) for item in pipeline]

            return pipeline

        @classmethod
        def convert_to_str(
            cls,
            value: Any,
        ) -> str:
            """
            Converts the given dictionary, list or primitive to a string.
            """

            return json.dumps(cls.prepare_pipeline(value))

        @classmethod
        def _merge_conditions(cls, conditions: list[Any]) -> Any:
            if len(conditions) == 1:
                return conditions[0]

            return {"$and": conditions}

        @classmethod
        def _get_compare_statement(cls, comparator: Callable, arg1: Any, arg2: Any) -> dict:
            """
            Returns the comparison statement in MongoDB syntax:

            .. code::

                {
                    "field": {
                        "$gt": "some_value",
                    }
                }
            """
            return {
                arg1: {
                    cls._compare_statements[comparator]: arg2,
                },
            }

        @classmethod
        def _validate_top_level_keys_in_where_parameter(cls, key: str):
            """
            Checks the 'where' parameter for illegal operators, such as ``$match``, ``$merge`` or ``$changeStream``.

            'where' clause can contain only filtering operators, like ``{"col1" {"$eq": 1}}`` or ``{"$and": [...]}``.
            """
            if key.startswith("$"):
                if key == "$match":
                    raise ValueError(
                        "'$match' operator not allowed at the top level of the 'where' parameter dictionary. "
                        "This error most likely occurred due to the fact that you used the MongoDB format for the "
                        "pipeline {'$match': {'column': ...}}. In the onETL paradigm, you do not need to specify the "
                        "'$match' keyword, but write the filtering condition right away, like {'column': ...}",
                    )
                if key in _upper_level_operators:  # noqa: WPS220
                    raise ValueError(  # noqa: WPS220
                        f"An invalid parameter {key!r} was specified in the 'where' "
                        "field. You cannot use aggregations or 'groupBy' clauses in 'where'",
                    )

    @slot
    def pipeline(
        self,
        collection: str,
        pipeline: dict | list[dict],
        df_schema: StructType | None = None,
        options: PipelineOptions | dict | None = None,
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
        pipeline = self.Dialect.prepare_pipeline(pipeline)
        log_with_indent(log, "collection = %r", collection)
        log_json(log, pipeline, name="pipeline")

        if df_schema:
            empty_df = self.spark.createDataFrame([], df_schema)
            log_dataframe_schema(log, empty_df)

        log_options(log, read_options)

        read_options["collection"] = collection
        read_options["aggregation.pipeline"] = self.Dialect.convert_to_str(pipeline)
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
    def get_min_max_bounds(
        self,
        source: str,
        column: str,
        expression: str | None = None,  # noqa: U100
        hint: dict | None = None,  # noqa: U100
        where: dict | None = None,
        options: ReadOptions | dict | None = None,
    ) -> tuple[Any, Any]:
        log.info("|Spark| Getting min and max values for column %r", column)

        read_options = self.ReadOptions.parse(options).dict(by_alias=True, exclude_none=True)

        # The '_id' field must be specified in the request.
        pipeline = [{"$group": {"_id": 1, "min": {"$min": f"${column}"}, "max": {"$max": f"${column}"}}}]
        if where:
            pipeline.insert(0, {"$match": where})

        pipeline = self.Dialect.prepare_pipeline(pipeline)

        read_options["connection.uri"] = self.connection_url
        read_options["collection"] = source
        read_options["aggregation.pipeline"] = self.Dialect.convert_to_str(pipeline)

        if hint:
            read_options["hint"] = self.Dialect.convert_to_str(hint)

        log.info("|%s| Executing aggregation pipeline:", self.__class__.__name__)
        log_with_indent(log, "collection = %r", source)
        log_json(log, pipeline, "pipeline")
        log_json(log, hint, "hint")

        df = self.spark.read.format("mongodb").options(**read_options).load()
        row = df.collect()[0]
        min_value = row["min"]
        max_value = row["max"]

        log.info("|Spark| Received values:")
        log_with_indent(log, "MIN(%s) = %r", column, min_value)
        log_with_indent(log, "MAX(%s) = %r", column, max_value)

        return min_value, max_value

    @slot
    def read_source_as_df(
        self,
        source: str,
        columns: list[str] | None = None,
        hint: dict | None = None,
        where: dict | None = None,
        df_schema: StructType | None = None,
        start_from: Statement | None = None,
        end_at: Statement | None = None,
        options: ReadOptions | dict | None = None,
    ) -> DataFrame:
        read_options = self.ReadOptions.parse(options).dict(by_alias=True, exclude_none=True)
        final_where = self.Dialect._condition_assembler(
            condition=where,
            start_from=start_from,
            end_at=end_at,
        )
        pipeline = self.Dialect.prepare_pipeline({"$match": final_where}) if final_where else None

        if pipeline:
            read_options["aggregation.pipeline"] = self.Dialect.convert_to_str(pipeline)

        if hint:
            read_options["hint"] = self.Dialect.convert_to_str(hint)

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

        log.info("|Spark| DataFrame successfully created from SQL statement ")
        return df

    @slot
    def write_df_to_target(
        self,
        df: DataFrame,
        target: str,
        options: WriteOptions | dict | None = None,
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
