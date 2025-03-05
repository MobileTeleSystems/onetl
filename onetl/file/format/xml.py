# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar

try:
    from pydantic.v1 import Field
except (ImportError, AttributeError):
    from pydantic import Field  # type: ignore[no-redef, assignment]

from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.spark import get_spark_version
from onetl._util.version import Version
from onetl.exception import MISSING_JVM_CLASS_MSG
from onetl.file.format.file_format import ReadWriteFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import Column, SparkSession
    from pyspark.sql.types import StructType


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

        * Spark versions: 3.2.x - 3.5.x
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

    .. versionadded:: 0.9.5

    Examples
    --------
    Describe options how to read from/write to XML file with specific options:

    .. code:: python

        from onetl.file.format import XML
        from pyspark.sql import SparkSession

        # Create Spark session with XML package loaded
        maven_packages = XML.get_packages(spark_version="3.5.5")
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
        spark_version: str,
        scala_version: str | None = None,
        package_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        .. versionadded:: 0.9.5

        Parameters
        ----------
        spark_version : str
            Spark version in format ``major.minor.patch``.

        scala_version : str, optional
            Scala version in format ``major.minor``.

            If ``None``, ``spark_version`` is used to determine Scala version.

        package_version : str, optional
            Package version in format ``major.minor.patch``. Default is ``0.18.0``.

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

            XML.get_packages(spark_version="3.5.5")
            XML.get_packages(spark_version="3.5.5", scala_version="2.12")
            XML.get_packages(
                spark_version="3.5.5",
                scala_version="2.12",
                package_version="0.18.0",
            )

        """

        if package_version:
            version = Version(package_version).min_digits(3)
            if version < Version("0.14"):
                raise ValueError(f"Package version must be above 0.13, got {version}")
            log.warning("Passed custom package version %r, it is not guaranteed to be supported", package_version)
        else:
            version = Version("0.18.0")

        spark_ver = Version(spark_version)
        scala_ver = Version(scala_version).min_digits(2) if scala_version else get_default_scala_version(spark_ver)

        # Ensure compatibility with Spark and Scala versions
        if spark_ver < Version("3.0"):
            raise ValueError(f"Spark version must be 3.x, got {spark_ver}")

        if scala_ver < Version("2.12"):
            raise ValueError(f"Scala version must be at least 2.12, got {scala_ver.format('{0}.{1}')}")

        return [f"com.databricks:spark-xml_{scala_ver.format('{0}.{1}')}:{version}"]

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

    def parse_column(self, column: str | Column, schema: StructType) -> Column:
        """
        Parses an XML string column into a structured Spark SQL column using the ``from_xml`` function
        provided by the `Databricks Spark XML library <https://github.com/databricks/spark-xml#pyspark-notes>`_
        based on the provided schema.

        .. note::

            This method assumes that the ``spark-xml`` package is installed: :obj:`XML.get_packages <onetl.file.format.xml.XML.get_packages>`.

        .. note::

            This method parses each DataFrame row individually. Therefore, for a specific column, each row must contain exactly one occurrence of the ``rowTag`` specified.
            If your XML data includes a root tag that encapsulates multiple row tags, you can adjust the schema to use an ``ArrayType`` to keep all child elements under the single root.

            .. code-block:: xml

                <books>
                    <book><title>Book One</title><author>Author A</author></book>
                    <book><title>Book Two</title><author>Author B</author></book>
                </books>

            And the corresponding schema in Spark using an ``ArrayType``:

            .. code-block:: python

                from pyspark.sql.types import StructType, StructField, ArrayType, StringType
                from onetl.file.format import XML

                # each DataFrame row has exactly one <books> tag
                xml = XML(rowTag="books")
                # each <books> tag have multiple <book> tags, so using ArrayType for such field
                schema = StructType(
                    [
                        StructField(
                            "book",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("title", StringType(), True),
                                        StructField("author", StringType(), True),
                                    ],
                                ),
                            ),
                            True,
                        ),
                    ],
                )

        .. versionadded:: 0.11.0

        Parameters
        ----------
        column : str | Column
            The name of the column or the column object containing XML strings/bytes to parse.

        schema : StructType
            The schema to apply when parsing the XML data. This defines the structure of the output DataFrame column.

        Returns
        -------
        Column with deserialized data, with the same structure as the provided schema. Column name is the same as input column.

        Examples
        --------

        >>> from pyspark.sql.types import StructType, StructField, IntegerType, StringType
        >>> from onetl.file.format import XML
        >>> df.show()
        +--+------------------------------------------------+
        |id|value                                           |
        +--+------------------------------------------------+
        |1 |<person><name>Alice</name><age>20</age></person>|
        |2 |<person><name>Bob</name><age>25</age></person>  |
        +--+------------------------------------------------+
        >>> df.printSchema()
        root
        |-- id: integer (nullable = true)
        |-- value: string (nullable = true)
        >>> xml = XML(rowTag="person")
        >>> xml_schema = StructType(
        ...     [
        ...         StructField("name", StringType(), nullable=True),
        ...         StructField("age", IntegerType(), nullable=True),
        ...     ],
        ... )
        >>> parsed_df = df.select("key", xml.parse_column("value", xml_schema))
        >>> parsed_df.show()
        +--+-----------+
        |id|value      |
        +--+-----------+
        |1 |{Alice, 20}|
        |2 |  {Bob, 25}|
        +--+-----------+
        >>> parsed_df.printSchema()
        root
        |-- id: integer (nullable = true)
        |-- value: struct (nullable = true)
        |    |-- name: string (nullable = true)
        |    |-- age: integer (nullable = true)
        """
        from pyspark.sql import Column, SparkSession  # noqa: WPS442

        spark = SparkSession._instantiatedSession  # noqa: WPS437
        self.check_if_supported(spark)

        from pyspark.sql.column import _to_java_column  # noqa: WPS450
        from pyspark.sql.functions import col

        if isinstance(column, Column):
            column_name, column = column._jc.toString(), column.cast("string")  # noqa: WPS437
        else:
            column_name, column = column, col(column).cast("string")

        java_column = _to_java_column(column)
        java_schema = spark._jsparkSession.parseDataType(schema.json())  # noqa: WPS437
        scala_options = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(  # noqa: WPS219, WPS437
            self.dict(),
        )
        jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(  # noqa: WPS219, WPS437
            java_column,
            java_schema,
            scala_options,
        )
        return Column(jc).alias(column_name)
