# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, ClassVar, Dict, Optional

try:
    from pydantic.v1 import Field, validator
except (ImportError, AttributeError):
    from pydantic import Field, validator  # type: ignore[no-redef, assignment]

from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.spark import get_spark_version
from onetl._util.version import Version
from onetl.exception import MISSING_JVM_CLASS_MSG
from onetl.file.format.file_format import ReadWriteFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrameReader, DataFrameWriter, SparkSession


PROHIBITED_OPTIONS = frozenset(
    (
        # These options should be passed in Spark session config, not file format options
        "spark.*",
    ),
)

READ_WRITE_OPTIONS = frozenset(
    ("positionalFieldMatching",),
)

READ_OPTIONS = frozenset(
    (
        "ignoreExtension",
        "datetimeRebaseMode",
    ),
)

WRITE_OPTIONS = frozenset(
    (
        "recordName",
        "recordNamespace",
        "compression",
    ),
)

log = logging.getLogger(__name__)


@support_hooks
class Avro(ReadWriteFileFormat):
    """
    Avro file format. |support_hooks|

    Based on `Spark Avro <https://spark.apache.org/docs/latest/sql-data-sources-avro.html>`_ file format.

    Supports reading/writing files with ``.avro`` extension.

    .. dropdown:: Version compatibility

        * Spark versions: 2.4.x - 3.5.x
        * Java versions: 8 - 20

        See documentation from link above.

    .. note ::

        You can pass any option to the constructor, even if it is not mentioned in this documentation.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version. See link above.

    .. versionadded:: 0.9.0

    Examples
    --------

    Describe options how to read from/write to Avro file with specific options:

    .. code:: python

        from onetl.file.format import Avro
        from pyspark.sql import SparkSession

        # Create Spark session with Avro package loaded
        maven_packages = Avro.get_packages(spark_version="3.5.5")
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        # Describe file format
        schema = {
            "type": "record",
            "name": "Person",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"},
            ],
        }
        avro = Avro(schema_dict=schema, compression="snappy")

    """

    name: ClassVar[str] = "avro"

    schema_dict: Optional[Dict] = Field(default=None, alias="avroSchema")
    schema_url: Optional[str] = Field(default=None, alias="avroSchemaUrl")

    class Config:
        known_options = READ_WRITE_OPTIONS | READ_OPTIONS | WRITE_OPTIONS
        prohibited_options = PROHIBITED_OPTIONS
        extra = "allow"

    @slot
    @classmethod
    def get_packages(
        cls,
        spark_version: str,
        scala_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        See `Maven package index <https://mvnrepository.com/artifact/org.apache.spark/spark-avro>`_
        for all available packages.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        spark_version : str
            Spark version in format ``major.minor.patch``.

        scala_version : str, optional
            Scala version in format ``major.minor``.

            If ``None``, ``spark_version`` is used to determine Scala version.

        Examples
        --------

        .. code:: python

            from onetl.file.format import Avro

            Avro.get_packages(spark_version="3.5.5")
            Avro.get_packages(spark_version="3.5.5", scala_version="2.12")

        """

        spark_ver = Version(spark_version).min_digits(3)
        if spark_ver < Version("2.4"):
            raise ValueError(f"Spark version should be at least 2.4, got {spark_version}")

        scala_ver = Version(scala_version).min_digits(2) if scala_version else get_default_scala_version(spark_ver)
        if scala_ver < Version("2.11"):
            raise ValueError(f"Scala version should be at least 2.11, got {scala_ver.format('{0}.{1}')}")

        return [f"org.apache.spark:spark-avro_{scala_ver.format('{0}.{1}')}:{spark_ver.format('{0}.{1}.{2}')}"]

    @slot
    def check_if_supported(self, spark: SparkSession) -> None:
        java_class = "org.apache.spark.sql.avro.AvroFileFormat"

        try:
            try_import_java_class(spark, java_class)
        except Exception as e:
            spark_version = get_spark_version(spark)
            msg = MISSING_JVM_CLASS_MSG.format(
                java_class=java_class,
                package_source=self.__class__.__name__,
                args=f"spark_version='{spark_version}'",
            )
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Missing Java class", exc_info=e, stack_info=True)
            raise ValueError(msg) from e

    @slot
    def apply_to_reader(self, reader: DataFrameReader) -> DataFrameReader:
        options = self.dict(by_alias=True, exclude_none=True, exclude={"schema"})
        if self.schema_dict:
            options["avroSchema"] = json.dumps(self.schema_dict)
        return reader.format(self.name).options(**options)

    @slot
    def apply_to_writer(self, writer: DataFrameWriter) -> DataFrameWriter:
        options = self.dict(by_alias=True, exclude_none=True, exclude={"schema"})
        if self.schema_dict:
            options["avroSchema"] = json.dumps(self.schema_dict)
        return writer.format(self.name).options(**options)

    def parse_column(self, column: str | Column) -> Column:
        """
        Parses an Avro binary column into a structured Spark SQL column using Spark's
        `from_avro <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.avro.functions.from_avro.html>`_ function, based on the schema provided within the class.

        .. note::

            Can be used only with Spark 3.x+

        .. warning::

            If ``schema_url`` is provided, ``requests`` library is used to fetch the schema from the URL. It should be installed manually, like this:

            .. code:: bash

                pip install requests

        .. versionadded:: 0.11.0

        Parameters
        ----------
        column : str | Column
            The name of the column or the column object containing Avro bytes to deserialize.
            Schema should match the provided Avro schema.

        Returns
        -------
        Column with deserialized data. Schema is matching the provided Avro schema. Column name is the same as input column.

        Raises
        ------
        ValueError
            If the Spark version is less than 3.x or if neither schema_dict nor schema_url is defined.
        ImportError
            If ``schema_url`` is used and the ``requests`` library is not installed.

        Examples
        --------

        >>> from pyspark.sql.functions import decode
        >>> from onetl.file.format import Avro
        >>> df.show()
        +----+----------------------+----------+---------+------+-----------------------+-------------+
        |key |value                 |topic     |partition|offset|timestamp              |timestampType|
        +----+----------------------+----------+---------+------+-----------------------+-------------+
        |[31]|[0A 41 6C 69 63 65 28]|topicAvro |0        |0     |2024-04-24 13:02:25.911|0            |
        |[32]|[06 42 6F 62 32]      |topicAvro |0        |1     |2024-04-24 13:02:25.922|0            |
        +----+----------------------+----------+---------+------+-----------------------+-------------+
        >>> df.printSchema()
        root
        |-- key: binary (nullable = true)
        |-- value: binary (nullable = true)
        |-- topic: string (nullable = true)
        |-- partition: integer (nullable = true)
        |-- offset: integer (nullable = true)
        |-- timestamp: timestamp (nullable = true)
        |-- timestampType: integer (nullable = true)
        >>> avro = Avro(
        ...     schema_dict={
        ...         "type": "record",
        ...         "name": "Person",
        ...         "fields": [
        ...             {"name": "name", "type": "string"},
        ...             {"name": "age", "type": "int"},
        ...         ],
        ...     }
        ... )
        >>> parsed_df = df.select(decode("key", "UTF-8").alias("key"), avro.parse_column("value"))
        >>> parsed_df.show(truncate=False)
        +---+-----------+
        |key|value      |
        +---+-----------+
        |1  |{Alice, 20}|
        |2  |{Bob, 25}  |
        +---+-----------+
        >>> parsed_df.printSchema()
        root
        |-- key: string (nullable = true)
        |-- value: struct (nullable = true)
        |    |-- name: string (nullable = true)
        |    |-- age: integer (nullable = true)
        """
        from pyspark.sql import Column, SparkSession  # noqa: WPS442
        from pyspark.sql.functions import col

        spark = SparkSession._instantiatedSession  # noqa: WPS437
        self.check_if_supported(spark)
        self._check_spark_version_for_serialization(spark)

        from pyspark.sql.avro.functions import from_avro

        if isinstance(column, Column):
            column_name = column._jc.toString()  # noqa: WPS437
        else:
            column_name, column = column, col(column).cast("binary")

        schema = self._get_schema_json()
        if not schema:
            raise ValueError("Avro.parse_column can be used only with defined `schema_dict` or `schema_url`")

        return from_avro(column, schema).alias(column_name)

    def serialize_column(self, column: str | Column) -> Column:
        """
        Serializes a structured Spark SQL column into an Avro binary column using Spark's
        `to_avro <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.avro.functions.to_avro.html#pyspark.sql.avro.functions.to_avro>`_ function.

        .. note::

            Can be used only with Spark 3.x+

        .. warning::

            If ``schema_url`` is provided, ``requests`` library is used to fetch the schema from the URL. It should be installed manually, like this:

            .. code:: bash

                pip install requests

        .. versionadded:: 0.11.0

        Parameters
        ----------
        column : str | Column
            The name of the column or the column object containing the data to serialize to Avro format.

        Returns
        -------
        Column with binary Avro data. Column name is the same as input column.

        Raises
        ------
        ValueError
            If the Spark version is less than 3.x.
        ImportError
            If ``schema_url`` is used and the ``requests`` library is not installed.

        Examples
        --------

        >>> from pyspark.sql.functions import decode
        >>> from onetl.file.format import Avro
        >>> df.show()
        +---+-----------+
        |key|value      |
        +---+-----------+
        |1  |{Alice, 20}|
        |2  |  {Bob, 25}|
        +---+-----------+
        >>> df.printSchema()
        root
        |-- key: string (nullable = true)
        |-- value: struct (nullable = true)
        |    |-- name: string (nullable = true)
        |    |-- age: integer (nullable = true)
        >>> # serializing data into Avro format
        >>> avro = Avro(
        ...     schema_dict={
        ...         "type": "record",
        ...         "name": "Person",
        ...         "fields": [
        ...             {"name": "name", "type": "string"},
        ...             {"name": "age", "type": "int"},
        ...         ],
        ...     }
        ... )
        >>> serialized_df = df.select("key", avro.serialize_column("value"))
        >>> serialized_df.show(truncate=False)
        +---+----------------------+
        |key|value                 |
        +---+----------------------+
        |  1|[0A 41 6C 69 63 65 28]|
        |  2|[06 42 6F 62 32]      |
        +---+----------------------+
        >>> serialized_df.printSchema()
        root
        |-- key: string (nullable = true)
        |-- value: binary (nullable = true)
        """
        from pyspark.sql import Column, SparkSession  # noqa: WPS442
        from pyspark.sql.functions import col

        spark = SparkSession._instantiatedSession  # noqa: WPS437
        self.check_if_supported(spark)
        self._check_spark_version_for_serialization(spark)

        from pyspark.sql.avro.functions import to_avro

        if isinstance(column, Column):
            column_name = column._jc.toString()  # noqa:  WPS437
        else:
            column_name, column = column, col(column)

        schema = self._get_schema_json()
        return to_avro(column, schema).alias(column_name)

    @validator("schema_dict", pre=True)
    def _parse_schema_from_json(cls, value):
        if isinstance(value, (str, bytes)):
            return json.loads(value)
        return value

    def _check_spark_version_for_serialization(self, spark: SparkSession):
        spark_version = get_spark_version(spark)
        if spark_version.major < 3:
            class_name = self.__class__.__name__
            error_msg = (
                f"`{class_name}.parse_column` or `{class_name}.serialize_column` are available "
                f"only since Spark 3.x, but got {spark_version}."
            )
            raise ValueError(error_msg)

    def _get_schema_json(self) -> str:
        if self.schema_dict:
            return json.dumps(self.schema_dict)
        elif self.schema_url:
            try:
                import requests

                response = requests.get(self.schema_url)  # noqa: S113
                return response.text
            except ImportError as e:
                raise ImportError(
                    "The 'requests' library is required to use 'schema_url' but is not installed. "
                    "Install it with 'pip install requests' or avoid using 'schema_url'.",
                ) from e
        else:
            return ""
