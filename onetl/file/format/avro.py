# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, ClassVar, Dict, Optional

from pydantic import Field, validator

from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.spark import get_spark_version
from onetl._util.version import Version
from onetl.exception import MISSING_JVM_CLASS_MSG
from onetl.file.format.file_format import ReadWriteFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import DataFrameReader, DataFrameWriter, SparkSession

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
        * Scala versions: 2.11 - 2.13

        See documentation from link above.

    .. note ::

        You can pass any option to the constructor, even if it is not mentioned in this documentation.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version. See link above.

    Examples
    --------

    Describe options how to read from/write to Avro file with specific options:

    .. code:: python

        from onetl.file.format import Avro
        from pyspark.sql import SparkSession

        # Create Spark session with Avro package loaded
        maven_packages = Avro.get_packages(spark_version="3.5.0")
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        # Describe file format
        schema = {
            "type": "record",
            "name": "Person",
            "fields": [{"name": "name", "type": "string"}],
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
        spark_version: str | Version,
        scala_version: str | Version | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        See `Maven package index <https://mvnrepository.com/artifact/org.apache.spark/spark-avro>`_
        for all available packages.

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

            Avro.get_packages(spark_version="3.2.4")
            Avro.get_packages(spark_version="3.2.4", scala_version="2.13")

        """

        spark_ver = Version.parse(spark_version)
        if spark_ver < (2, 4):
            raise ValueError(f"Spark version should be at least 2.4, got {spark_version}")

        scala_ver = Version.parse(scala_version) if scala_version else get_default_scala_version(spark_ver)
        if scala_ver.digits(2) < (2, 11):
            raise ValueError(f"Scala version should be at least 2.11, got {scala_ver}")

        return [f"org.apache.spark:spark-avro_{scala_ver.digits(2)}:{spark_ver.digits(3)}"]

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

    @validator("schema_dict", pre=True)
    def _parse_schema_from_json(cls, value):
        if isinstance(value, (str, bytes)):
            return json.loads(value)
        return value
