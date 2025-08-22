# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import logging
import warnings
from typing import TYPE_CHECKING, ClassVar, Optional, Union

from typing_extensions import Literal

try:
    from pydantic.v1 import Field, root_validator, validator
except (ImportError, AttributeError):
    from pydantic import Field, validator, root_validator  # type: ignore[no-redef, assignment]

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

PARSE_COLUMN_UNSUPPORTED_OPTIONS = {
    "compression",
}

SERIALIZE_COLUMN_UNSUPPORTED_OPTIONS = {
    "compression",
    "mode",
    "positionalFieldMatching",
    "enableStableIdentifiersForUnionType",
}

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

    .. versionadded:: 0.9.0

    Examples
    --------

    .. note ::

        You can pass any option mentioned in
        `official documentation <https://spark.apache.org/docs/latest/sql-data-sources-avro.html>`_.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version.

    .. tabs::

        .. code-tab:: py Reading files

            from pyspark.sql import SparkSession
            from onetl.file.format import Avro

            # Create Spark session with Avro package loaded
            maven_packages = Avro.get_packages(spark_version="3.5.6")
            spark = (
                SparkSession.builder.appName("spark-app-name")
                .config("spark.jars.packages", ",".join(maven_packages))
                .getOrCreate()
            )

            schema = {
                "type": "record",
                "name": "Person",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                ],
            }
            avro = Avro(avroSchema=schema)  # or avroSchemaUrl=...

        .. code-tab:: py Writing files

            # Create Spark session with Avro package loaded
            spark = ...

            from onetl.file.format import Avro

            schema = {
                "type": "record",
                "name": "Person",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                ],
            }
            avro = Avro(
                avroSchema=schema,  # or avroSchemaUrl=...
                compression="snappy",
            )

    """

    name: ClassVar[str] = "avro"

    schema_dict: Optional[dict] = Field(default=None, alias="avroSchema")
    """
    Avro schema in JSON format representation.

    .. code:: python

        avro = Avro(
            avroSchema={
                "type": "record",
                "name": "Person",
                "fields": [
                    {"name": "name", "type": "string"},
                    {"name": "age", "type": "int"},
                ],
            },
        )

    If set, all records should match this schema.

    .. warning::

        Mutually exclusive with :obj:`~schema_url`.
    """

    schema_url: Optional[str] = Field(default=None, alias="avroSchemaUrl")
    """
    URL to Avro schema in JSON format. Usually points to Schema Registry, like:

    .. code:: python

        schema_registry = "http://some.schema.registry.domain"
        name = "MyAwesomeSchema"
        version = "latest"

        schema_url = f"{schema_registry}/subjects/{name}/versions/{version}/schema"
        avro = Avro(avroSchemaUrl=schema_url)

    If set, schema is fetched before any records are parsed, so all records should match this schema.

    .. warning::

        Mutually exclusive with :obj:`~schema_dict`.
    """

    recordName: Optional[str] = None
    """
    Record name in written Avro schema.
    Default is ``topLevelRecord``.

    .. note::

        Used only for writing files and by :obj:`~serialize_column`.
    """

    recordNamespace: Optional[str] = None
    """
    Record namespace in written Avro schema. Default is not set.

    .. note::

        Used only for writing files and by :obj:`~serialize_column`.
    """

    compression: Union[str, Literal["uncompressed", "snappy", "deflate", "bzip2", "xz", "zstandard"], None] = None
    """
    Compression codec.
    By default, Spark config value ``spark.sql.avro.compression.codec `` (``snappy``) is used.

    .. note::

        Used only for writing files. Ignored by :obj:`~serialize_column`.
    """

    mode: Optional[Literal["PERMISSIVE", "FAILFAST"]] = None
    """
    How to handle parsing errors:
      * ``PERMISSIVE`` - set field value as ``null``.
      * ``FAILFAST`` - throw an error immediately.

    Default is ``FAILFAST``.

    .. note::

        Used only by :obj:`~parse_column` method.
    """

    datetimeRebaseMode: Optional[Literal["CORRECTED", "LEGACY", "EXCEPTION"]] = None
    """
    While converting dates/timestamps from Julian to Proleptic Gregorian calendar, handle value ambiguity:
      * ``EXCEPTION`` - fail if ancient dates/timestamps are ambiguous between the two calendars.
      * ``CORRECTED`` - load dates/timestamps without as-is.
      * ``LEGACY`` - rebase ancient dates/timestamps from the Julian to Proleptic Gregorian calendar.

    By default, Spark config value ``spark.sql.avro.datetimeRebaseModeInRead`` (``CORRECTED``) is used.

    .. note::

        Used only for reading files and by :obj:`~parse_column`.
    """

    positionalFieldMatching: Optional[bool] = None
    """
    If ``True``, match Avro schema field and DataFrame column by position.
    If ``False``, match by name.

    Default is ``False``.
    """

    enableStableIdentifiersForUnionType: Optional[bool] = None
    """
    Avro schema may contain union types, which are not supported by Spark.
    Different variants of union are split to separated DataFrame columns with respective type.

    If option value is ``True``, DataFrame column names are based on Avro variant names, e.g. ``member_int``, ``member_string``.
    If ``False``, DataFrame column names are generated using field position, e.g. ``member0``, ``member1``.

    Default is ``False``.

    .. note::

        Used only for reading files and by :obj:`~parse_column`.
    """

    class Config:
        known_options = frozenset()
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

            Avro.get_packages(spark_version="3.5.6")
            Avro.get_packages(spark_version="3.5.6", scala_version="2.12")

        """

        spark_ver = Version(spark_version).min_digits(3)
        scala_ver = Version(scala_version).min_digits(2) if scala_version else get_default_scala_version(spark_ver)
        return [f"org.apache.spark:spark-avro_{scala_ver.format('{0}.{1}')}:{spark_ver.format('{0}.{1}.{2}')}"]

    @slot
    def check_if_supported(self, spark: SparkSession) -> None:
        java_class = "org.apache.spark.sql.avro.AvroFileFormat"

        try:
            try_import_java_class(spark, java_class)
        except Exception as e:
            spark_version = get_spark_version(spark).format("{0}.{1}.{2}")
            msg = MISSING_JVM_CLASS_MSG.format(
                java_class=java_class,
                package_source=self.__class__.__name__,
                args=f"spark_version='{spark_version}'",
            )
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
        `from_avro <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.avro.functions.from_avro.html>`_ function,
        based on the schema provided within the class.

        .. note::

            Can be used only with Spark 3.x+

        .. warning::

            If ``schema_url`` is provided, ``requests`` library is used to fetch the schema from the URL.
            It should be installed manually, like this:

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
            If the Spark version is less than 3.x or if neither ``avroSchema`` nor ``avroSchemaUrl`` are defined.
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
        ...     avroSchema={  # or avroSchemaUrl=...
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
        self._check_unsupported_parse_options()

        from pyspark.sql.avro.functions import from_avro

        if isinstance(column, Column):
            column_name = column._jc.toString()  # noqa: WPS437
        else:
            column_name, column = column, col(column).cast("binary")

        schema = self._get_schema_json()
        if not schema:
            raise ValueError("Avro.parse_column can be used only with defined `avroSchema` or `avroSchemaUrl`")

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
        ...     avroSchema={  # or avroSchemaUrl=...
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
        self._check_unsupported_serialization_options()

        from pyspark.sql.avro.functions import to_avro

        if isinstance(column, Column):
            column_name = column._jc.toString()  # noqa:  WPS437
        else:
            column_name, column = column, col(column)

        schema = self._get_schema_json()
        return to_avro(column, schema).alias(column_name)

    def __repr__(self):
        options_dict = self.dict(by_alias=True, exclude_none=True)
        options_kwargs = ", ".join(f"{k}={v!r}" for k, v in options_dict.items())
        return f"{self.__class__.__name__}({options_kwargs})"

    def _check_unsupported_parse_options(self):
        current_options = self.dict(by_alias=True, exclude_none=True)
        unsupported_options = current_options.keys() & PARSE_COLUMN_UNSUPPORTED_OPTIONS
        if unsupported_options:
            warnings.warn(
                f"Options `{sorted(unsupported_options)}` are set but not supported by `Avro.parse_column`.",
                UserWarning,
                stacklevel=2,
            )

    def _check_unsupported_serialization_options(self):
        current_options = self.dict(by_alias=True, exclude_none=True)
        unsupported_options = current_options.keys() & SERIALIZE_COLUMN_UNSUPPORTED_OPTIONS
        if unsupported_options:
            warnings.warn(
                f"Options `{sorted(unsupported_options)}` are set but not supported by `Avro.serialize_column`.",
                UserWarning,
                stacklevel=2,
            )

    @validator("schema_dict", pre=True)
    def _parse_schema_from_json(cls, value):
        if isinstance(value, (str, bytes)):
            return json.loads(value)
        return value

    @root_validator(pre=True)
    def _check_schema(cls, values):
        schema_dict = values.get("schema_dict")
        schema_url = values.get("schema_url")
        if schema_dict and schema_url:
            raise ValueError("Parameters `avroSchema` and `avroSchemaUrl` are mutually exclusive.")
        return values

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
