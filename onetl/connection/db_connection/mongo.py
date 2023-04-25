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
import operator
from datetime import datetime
from enum import Enum
from string import Template
from typing import (  # noqa: WPS235
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    Iterable,
    Mapping,
    Optional,
)
from urllib import parse as parser

from etl_entities.instance import Host
from pydantic import SecretStr

from onetl.base.base_db_connection import BaseDBConnection
from onetl.connection.db_connection.dialect_mixins import SupportHWMExpressionNone
from onetl.connection.db_connection.dialect_mixins.support_table_without_dbschema import (
    SupportTableWithoutDBSchema,
)
from onetl.hwm import Statement
from onetl.log import log_with_indent

if TYPE_CHECKING:
    from pyspark.sql.types import StructType
    from pyspark.sql import DataFrame

from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.dialect_mixins import (
    SupportColumnsNone,
    SupportDfSchemaStruct,
)
from onetl.exception import MISSING_JVM_CLASS_MSG
from onetl.impl import GenericOptions

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


class MongoDB(DBConnection):
    """MongoDB connection.

    Based on package ``org.mongodb.spark:mongo-spark-connector``
    (`MongoDB connector for Spark <https://www.mongodb.com/docs/spark-connector/master/>`_)

    .. dropdown:: Version compatibility

        * MongoDB server versions: 2.6 - 4.2
        * Spark versions: 2.3.x - 3.2.x
        * Java versions: 8 - 17

        See `official documentation <https://www.mongodb.com/docs/spark-connector/master/>`_.

    .. warning::

        To use Greenplum connector you should have PySpark installed (or injected to ``sys.path``)
        BEFORE creating the connector instance.

        You can install PySpark as follows:

        .. code:: bash

            pip install onetl[spark]  # latest PySpark version

            # or
            pip install onetl pyspark=3.3.2  # pass specific PySpark version

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
        # MongoDB.package_spark_2_3
        # MongoDB.package_spark_2_4
        # MongoDB.package_spark_3_2
        # MongoDB.package_spark_3_3

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

    class Extra(GenericOptions):
        class Config:
            extra = "allow"

    database: str
    host: Host
    user: str
    password: SecretStr
    port: int = 27017
    extra: Extra = Extra()
    package_spark_2_4: ClassVar[str] = "org.mongodb.spark:mongo-spark-connector_2.11:2.4.4"
    package_spark_2_3: ClassVar[str] = "org.mongodb.spark:mongo-spark-connector_2.11:2.3.6"
    package_spark_3_2: ClassVar[str] = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"
    package_spark_3_3: ClassVar[str] = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"

    class PipelineOptions(GenericOptions):
        """Aggregation pipeline options for MongoDB connector.

        The only difference from :obj:`~ReadOptions` that it is allowed to pass the 'hint' parameter.

        .. note ::

            You can pass any value
            `supported by connector <https://www.mongodb.com/docs/spark-connector/master/configuration/
            #:~:text=See%20Cache%20Configuration.-,Input%20Configuration,-You%20can%20configure>`_,
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
            `supported by connector <https://www.mongodb.com/docs/spark-connector/master/configuration/
            #:~:text=See%20Cache%20Configuration.-,Input%20Configuration,-You%20can%20configure>`_,
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
        def convert_filter_parameter_to_pipeline(
            cls,
            parameter: Any,
        ) -> str:  # noqa: WPS231
            """
            Converts the given dictionary, list or primitive to a string. for each element of the collection,
            the method calls itself and internally processes each element depending on its type.
            """
            if isinstance(parameter, Mapping):
                return cls._build_string_pipeline_from_dictionary(parameter)

            if isinstance(parameter, Iterable) and not isinstance(parameter, str):
                return cls._build_string_pipeline_from_list(parameter)

            return cls._build_pipeline_from_simple_types(parameter)

        @classmethod
        def generate_where_request(cls, where: dict) -> str:  # noqa: WPS231
            """
            The passed dict is converted to a MongoDb-readable format, the original result is passed to the
            pipeline.

            """
            blank_pipeline = Template("{'$$match':$custom_condition}")
            return blank_pipeline.substitute(custom_condition=cls.convert_filter_parameter_to_pipeline(where))

        @classmethod
        def _where_condition(cls, result: list) -> Optional[dict]:
            result = list(filter(None, result))

            if not result:
                return None

            if len(result) > 1:
                return {"$and": result}

            return result[0]

        @classmethod
        def _get_compare_statement(cls, comparator: Callable, arg1: Any, arg2: Any) -> dict:
            result_statement = {}
            # The value with which the field is compared is substituted into the dictionary:
            # {"$some_condition": None} => {"$some_condition": "some_value"}
            # If the type is variable datetime then there will be a conversion:
            # {"$some_condition": None} => {"$some_condition": {"$date": "some_value"}}
            result_statement[arg1] = {
                cls._compare_statements[comparator]: cls._serialize_datetime_value(arg2),
            }
            return result_statement

        @classmethod
        def _serialize_datetime_value(cls, value: Any) -> str | int | dict:
            """
            Transform the value into an SQL Dialect-supported form.
            """
            if isinstance(value, datetime):
                # MongoDB requires UTC to be specified in queries.
                # NOTE: If you pass the date through ISODate, then you must specify
                # it in the format ISODate('2023-07-11T00:51:54Z').
                # Thus, you must write 'Z' at the end and not '+00:00' and milliseconds are not supported.
                return {"$date": value.astimezone().isoformat()}

            return value

        @classmethod
        def _build_string_pipeline_from_dictionary(cls, parameter: Mapping) -> str:
            """
            Converts the passed map to a string. Map elements can be collections. Loops through all elements of the map
            and converts each to a string depending on the type of each element.
            """
            list_of_operations = []
            for key, value in parameter.items():
                value = (
                    cls.convert_filter_parameter_to_pipeline(key)
                    + ":"
                    + cls.convert_filter_parameter_to_pipeline(value)
                )
                list_of_operations.append(value)
            return "{" + ",".join(list_of_operations) + "}"

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

        @classmethod
        def _build_string_pipeline_from_list(cls, param: Iterable) -> str:
            """
            The passed list is converted to a string. The elements of the list can be dictionaries. Iterates through all
            the elements of the list and processes each element by converting it to a string.
            """
            list_of_operations = []
            for elem in param:
                list_value = cls.convert_filter_parameter_to_pipeline(elem)
                list_of_operations.append(list_value)
            return "[" + ",".join(list_of_operations) + "]"

        @classmethod
        def _build_pipeline_from_simple_types(cls, param: Any) -> str:
            """
            Wraps the passed parameters in parentheses. Doesn't work with collections.
            """
            if isinstance(param, str):
                return "'" + param + "'"

            if isinstance(param, bool):
                return str(param).lower()

            if param is None:
                return "null"

            if isinstance(param, int):
                return str(param)

            raise ValueError(f"Unsupported value type : {param.__class__.__name__!r}")

    def pipeline(
        self,
        collection: str,
        pipeline: dict | list[dict],
        df_schema: StructType | None = None,
        options: PipelineOptions | dict | None = None,
    ):
        """
        Execute a pipeline for a specific collection, and return DataFrame.

        Almost like `Aggregation pipeline syntax <https://www.mongodb.com/docs/manual/core/aggregation-pipeline/>`_
        in MongoDB:

        .. code:: js

            db.collection.aggregate([{"$match": ...}, {"$group": ...}])

        but pipeline is executed on Spark executors, in a distributed way.

        .. note::

            This method does not support :ref:`strategy`, use :obj:`onetl.core.db_reader.db_reader.DBReader` instead

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
                        "_id": {},
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

        read_options = self.PipelineOptions.parse(options).dict(by_alias=True, exclude_none=True)
        read_options["pipeline"] = self.Dialect.convert_filter_parameter_to_pipeline(pipeline)
        read_options["spark.mongodb.input.uri"] = self.connection_url
        read_options["spark.mongodb.input.collection"] = collection
        spark_reader = self.spark.read.format("mongo").options(**read_options)

        if df_schema:
            spark_reader = spark_reader.schema(df_schema)

        return spark_reader.load()

    @property
    def instance_url(self) -> str:
        return f"{self.__class__.__name__.lower()}://{self.host}:{self.port}/{self.database}"

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

    def get_min_max_bounds(
        self,
        table: str,
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
        min_max_pipeline_dict = {"$group": {"_id": {}, "min": {"$min": f"${column}"}, "max": {"$max": f"${column}"}}}
        min_max_pipeline = self.Dialect.convert_filter_parameter_to_pipeline(min_max_pipeline_dict)

        if where is not None:
            pipeline = f"[{self.Dialect.generate_where_request(where=where)},{min_max_pipeline}]"
        else:
            pipeline = min_max_pipeline

        read_options["spark.mongodb.input.uri"] = self.connection_url
        read_options["spark.mongodb.input.collection"] = table
        read_options["pipeline"] = pipeline

        log.info("|%s| Getting a data frame from MongoDB with a pipeline:", self.__class__.__name__)
        log_with_indent(pipeline)

        min_max_df = self.spark.read.format("mongo").options(**read_options).load()
        # Fields 'max' and 'min' go in the reverse order specified in the pipeline.
        max_value, min_value = min_max_df.collect()[0]

        log.info("|Spark| Received values:")
        log_with_indent("MIN(%s) = %r", column, min_value)
        log_with_indent("MAX(%s) = %r", column, max_value)

        return min_value, max_value

    def read_table(
        self,
        table: str,
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

        pipeline = self.Dialect._condition_assembler(
            condition=where,
            start_from=start_from,
            end_at=end_at,
        )

        if pipeline:
            read_options["pipeline"] = self.Dialect.generate_where_request(pipeline)

        if hint:
            read_options["hint"] = self.Dialect.convert_filter_parameter_to_pipeline(hint)

        read_options["spark.mongodb.input.uri"] = self.connection_url
        read_options["spark.mongodb.input.collection"] = table
        spark_reader = self.spark.read.format("mongo").options(**read_options)

        if df_schema:
            spark_reader = spark_reader.schema(df_schema)

        df = spark_reader.load()

        if columns:
            return df.select(*columns)

        return df

    def save_df(
        self,
        df: DataFrame,
        table: str,
        options: WriteOptions | dict | None = None,
    ) -> None:
        self._check_driver_imported()
        write_options = self.WriteOptions.parse(options)
        mode = write_options.mode
        write_options = write_options.dict(by_alias=True, exclude_none=True, exclude={"mode"})
        write_options["spark.mongodb.output.uri"] = self.connection_url
        write_options["spark.mongodb.output.collection"] = table
        df.write.format("mongo").mode(mode).options(**write_options).save()

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
        # Connector v10.x
        class_name = "com.mongodb.spark.sql.connector.MongoTableProvider"
        missing_class = getattr(gateway.jvm, class_name)

        exception: Exception | None
        try:
            gateway.help(missing_class, display=False)
            exception = None
        except Exception as e:
            exception = e

        if exception:
            # Connector v3.x or lower
            class_name = "com.mongodb.spark.sql.DefaultSource"
            missing_class = getattr(gateway.jvm, class_name)
            try:
                gateway.help(missing_class, display=False)
                exception = None
            except Exception as e:
                exception = e

        if exception:
            log.error(
                MISSING_JVM_CLASS_MSG,
                class_name,
                f"{self.__class__.__name__}.package_spark_{spark_version}",
                exc_info=False,
            )
            raise exception
