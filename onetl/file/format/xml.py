# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar

from pydantic import Field

from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.spark import get_spark_version
from onetl._util.version import Version
from onetl.exception import MISSING_JVM_CLASS_MSG
from onetl.file.format.file_format import ReadWriteFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


PROHIBITED_OPTIONS = frozenset(
    (
        # filled by onETL classes
        "path",
    ),
)


READ_OPTIONS = frozenset(
    (
        "rowTag",
        "samplingRatio",
        "excludeAttribute",
        "treatEmptyValuesAsNulls",
        "mode",
        "inferSchema",
        "columnNameOfCorruptRecord",
        "attributePrefix",
        "valueTag",
        "charset",
        "ignoreSurroundingSpaces",
        "wildcardColName",
        "rowValidationXSDPath",
        "ignoreNamespace",
        "timestampFormat",
        "dateFormat",
    ),
)

WRITE_OPTIONS = frozenset(
    (
        "rowTag",
        "rootTag",
        "declaration",
        "arrayElementName",
        "nullValue",
        "attributePrefix",
        "valueTag",
        "compression",
        "timestampFormat",
        "dateFormat",
    ),
)


log = logging.getLogger(__name__)


@support_hooks
class XML(ReadWriteFileFormat):
    """
    XML file format. |support_hooks|

    Based on `Databricks Spark XML <https://github.com/databricks/spark-xml>`_ file format.

    Supports reading/writing files with ``.xml`` extension.

    .. warning::

        Due to `bug <https://github.com/databricks/spark-xml/issues/664>`_ written files currently does not have ``.xml`` extension.

    .. versionadded:: 0.9.5

    .. dropdown:: Version compatibility

        * Spark versions: 3.2.x - 3.5.x.
        * Scala versions: 2.12 - 2.13
        * Java versions: 8 - 20

        See documentation from link above.

    .. note ::

        You can pass any option to the constructor, even if it is not mentioned in this documentation.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version. See link above.

    .. warning::

        By default, reading is done using ``mode=PERMISSIVE`` which replaces columns with wrong data type or format with ``null`` values.
        Be careful while parsing values like timestamps, they should match the ``timestampFormat`` option.
        Using ``mode=FAILFAST`` will throw an exception instead of producing ``null`` values.
        `Follow <https://github.com/databricks/spark-xml/issues/662>`_

    Examples
    --------
    Describe options how to read from/write to XML file with specific options:

    .. code:: python

        from onetl.file.format import XML
        from pyspark.sql import SparkSession

        # Create Spark session with XML package loaded
        maven_packages = XML.get_packages(spark_version="3.5.0")
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        xml = XML(row_tag="item")

    """

    name: ClassVar[str] = "xml"

    row_tag: str = Field(alias="rowTag")

    class Config:
        known_options = READ_OPTIONS | WRITE_OPTIONS
        prohibited_options = PROHIBITED_OPTIONS
        extra = "allow"

    @slot
    @classmethod
    def get_packages(  # noqa: WPS231
        cls,
        spark_version: str | Version,
        scala_version: str | Version | None = None,
        package_version: str | Version | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        Parameters
        ----------
        spark_version : str
            Spark version in format ``major.minor.patch``.

        scala_version : str, optional
            Scala version in format ``major.minor``.

            If ``None``, ``spark_version`` is used to determine Scala version.

        package_version : str, optional
            Package version in format ``major.minor.patch``. Default is ``0.17.0``.

            See `Maven index <https://mvnrepository.com/artifact/com.databricks/spark-xml>`_
            for list of available versions.

            .. warning::

                Version ``0.13`` and below are not supported.

            .. note::

                It is not guaranteed that custom package versions are supported.
                Tests are performed only for default version.

        Examples
        --------

        .. code:: python

            from onetl.file.format import XML

            XML.get_packages(spark_version="3.5.0")
            XML.get_packages(spark_version="3.5.0", scala_version="2.12")
            XML.get_packages(
                spark_version="3.5.0",
                scala_version="2.12",
                package_version="0.17.0",
            )

        """

        if package_version:
            version = Version.parse(package_version)
            if version < (0, 14):
                raise ValueError(f"Package version must be above 0.13, got {version}")
            log.warning("Passed custom package version %r, it is not guaranteed to be supported", package_version)
        else:
            version = Version(0, 17, 0)

        spark_ver = Version.parse(spark_version)
        scala_ver = Version.parse(scala_version) if scala_version else get_default_scala_version(spark_ver)

        # Ensure compatibility with Spark and Scala versions
        if spark_ver < (3, 0):
            raise ValueError(f"Spark version must be 3.x, got {spark_ver}")

        if scala_ver < (2, 12) or scala_ver > (2, 13):
            raise ValueError(f"Scala version must be 2.12 or 2.13, got {scala_ver}")

        return [f"com.databricks:spark-xml_{scala_ver.digits(2)}:{version.digits(3)}"]

    @slot
    def check_if_supported(self, spark: SparkSession) -> None:
        java_class = "com.databricks.spark.xml.XmlReader"

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
