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
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Dict, Iterable, Mapping
from urllib import parse as parser

from etl_entities.instance import Host
from pydantic import SecretStr

from onetl.base.base_db_connection import BaseDBConnection
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.dialect_mixins import (
    SupportColumnsNone,
    SupportDfSchemaStruct,
    SupportHWMExpressionNone,
)
from onetl.connection.db_connection.dialect_mixins.support_table_without_dbschema import (
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


class MongoDBWriteMode(str, Enum):
    APPEND = "append"
    OVERWRITE = "overwrite"

    def __str__(self) -> str:
        return str(self.value)


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
        * Java versions: 8 - 17

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

        # Package should match your Spark version:
        # MongoDB.package_spark_3_2
        # MongoDB.package_spark_3_3
        # MongoDB.package_spark_3_4

        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", MongoDB.package_spark_3_2)
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

    class Extra(GenericOptions):
        class Config:
            extra = "allow"

    database: str
    host: Host
    user: str
    password: SecretStr
    port: int = 27017
    extra: Extra = Extra()
    package_spark_3_2: ClassVar[str] = "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"
    package_spark_3_3: ClassVar[str] = "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"
    package_spark_3_4: ClassVar[str] = "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1"

    class PipelineOptions(GenericOptions):
        """Aggregation pipeline options for MongoDB connector.

        The only difference from :obj:`~ReadOptions` that it is allowed to pass the 'hint' parameter.

        .. note ::

            You can pass any value
            `supported by connector <https://www.mongodb.com/docs/spark-connector/current/configuration/read>`_,
            even if it is not mentioned in this documentation.

            The set of supported options depends on connector version.

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

            The set of supported options depends on connector version.

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

    class Dialect(
        SupportTableWithoutDBSchema,
        SupportHWMExpressionNone,
        SupportColumnsNone,
        SupportDfSchemaStruct,
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

            This method does not support :ref:`strategy`, use :obj:`onetl.db.db_reader.db_reader.DBReader` instead

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
        self._check_driver_imported()

        log.info("|%s| Executing aggregation pipeline:", self.__class__.__name__)

        read_options = self.PipelineOptions.parse(options).dict(by_alias=True, exclude_none=True)
        pipeline = self.Dialect.prepare_pipeline(pipeline)
        log_with_indent("collection = %r", collection)
        log_json(pipeline, name="pipeline")

        if df_schema:
            empty_df = self.spark.createDataFrame([], df_schema)
            log_dataframe_schema(empty_df)

        log_options(read_options)

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
        self._check_driver_imported()
        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()

        try:
            jvm = self.spark._sc._gateway.jvm  # type: ignore
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
        self._check_driver_imported()

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
        log_with_indent("collection = %r", source)
        log_json(pipeline, "pipeline")
        log_json(hint, "hint")

        df = self.spark.read.format("mongodb").options(**read_options).load()
        row = df.collect()[0]
        min_value = row["min"]
        max_value = row["max"]

        log.info("|Spark| Received values:")
        log_with_indent("MIN(%s) = %r", column, min_value)
        log_with_indent("MAX(%s) = %r", column, max_value)

        return min_value, max_value

    @slot
    def read_df(
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
        self._check_driver_imported()

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
        log_with_indent("collection = %r", source)
        log_json(pipeline, "pipeline")
        log_json(hint, "hint")
        spark_reader = self.spark.read.format("mongodb").options(**read_options)

        if df_schema:
            spark_reader = spark_reader.schema(df_schema)

        df = spark_reader.load()

        if columns:
            df = df.select(*columns)

        log.info("|Spark| DataFrame successfully created from SQL statement ")
        return df

    @slot
    def write_df(
        self,
        df: DataFrame,
        target: str,
        options: WriteOptions | dict | None = None,
    ) -> None:
        self._check_driver_imported()
        write_options = self.WriteOptions.parse(options)
        mode = write_options.mode
        write_options = write_options.dict(by_alias=True, exclude_none=True, exclude={"mode"})
        write_options["connection.uri"] = self.connection_url
        write_options["collection"] = target

        log.info("|%s| Saving data to a collection %r", self.__class__.__name__, target)
        df.write.format("mongodb").mode(mode).options(**write_options).save()
        log.info("|%s| Collection %r is successfully written", self.__class__.__name__, target)

    @property
    def connection_url(self) -> str:
        prop = self.extra.dict(by_alias=True)
        parameters = "&".join(f"{k}={v}" for k, v in sorted(prop.items()))
        parameters = "?" + parameters if parameters else ""
        password = parser.quote(self.password.get_secret_value())
        return f"mongodb://{self.user}:{password}@{self.host}:{self.port}/{self.database}{parameters}"

    def _check_driver_imported(self):
        spark_version = "_".join(self.spark.version.split(".")[:2])

        gateway = self.spark._sc._gateway  # type: ignore
        class_name = "com.mongodb.spark.sql.connector.MongoTableProvider"
        missing_class = getattr(gateway.jvm, class_name)

        try:
            gateway.help(missing_class, display=False)
        except Exception as e:
            log.error(
                MISSING_JVM_CLASS_MSG,
                class_name,
                f"{self.__class__.__name__}.package_spark_{spark_version}",
                exc_info=False,
            )
            raise e
