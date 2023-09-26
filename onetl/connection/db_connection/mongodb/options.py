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

import warnings
from enum import Enum

from pydantic import Field, root_validator

from onetl.impl import GenericOptions

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


class MongoDBCollectionExistBehavior(str, Enum):
    APPEND = "append"
    IGNORE = "ignore"
    ERROR = "error"
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


class MongoDBPipelineOptions(GenericOptions):
    """Aggregation pipeline options for MongoDB connector.

    The only difference from :obj:`MongoDBReadOptions` that it is allowed to pass the ``hint`` parameter.

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


class MongoDBReadOptions(GenericOptions):
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


class MongoDBWriteOptions(GenericOptions):
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

        * ``ignore``
            Ignores the write operation if the collection already exists.

            .. dropdown:: Behavior in details

                * Collection does not exist
                    Collection is created using options provided by user

                * Collection exists
                    The write operation is ignored, and no data is written to the collection.

        * ``error``
            Raises an error if the collection already exists.

            .. dropdown:: Behavior in details

                * Collection does not exist
                    Collection is created using options provided by user

                * Collection exists
                    An error is raised, and no data is written to the collection.

    """

    class Config:
        prohibited_options = PROHIBITED_OPTIONS
        known_options = KNOWN_WRITE_OPTIONS
        extra = "allow"

    @root_validator(pre=True)
    def _mode_is_deprecated(cls, values):
        if "mode" in values:
            warnings.warn(
                "Option `MongoDB.WriteOptions(mode=...)` is deprecated since v0.9.0 and will be removed in v1.0.0. "
                "Use `MongoDB.WriteOptions(if_exists=...)` instead",
                category=UserWarning,
                stacklevel=3,
            )
        return values
