# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
import warnings
from typing import TYPE_CHECKING, ClassVar, Optional, Union

from typing_extensions import Literal

try:
    from pydantic.v1 import Field
except (ImportError, AttributeError):
    from pydantic import Field  # type: ignore[no-redef, assignment]

from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.spark import get_spark_version, stringify
from onetl._util.version import Version
from onetl.exception import MISSING_JVM_CLASS_MSG
from onetl.file.format.file_format import ReadWriteFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import Column, SparkSession
    from pyspark.sql.types import StructType


log = logging.getLogger(__name__)
PARSE_COLUMN_UNSUPPORTED_OPTIONS = {"inferSchema", "samplingRatio"}


@support_hooks
class XML(ReadWriteFileFormat):
    """
    XML file format. |support_hooks|

    Based on `Databricks Spark XML <https://github.com/databricks/spark-xml>`_ file format.

    Supports reading/writing files with ``.xml`` extension.

    .. versionadded:: 0.9.5

    .. dropdown:: Version compatibility

        * Spark versions: 3.2.x - 3.5.x
        * Java versions: 8 - 20

        See `official documentation <https://github.com/databricks/spark-xml>`_.

    Examples
    --------

    .. note ::

        You can pass any option mentioned in
        `official documentation <https://github.com/databricks/spark-xml>`_.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on ``spark-xml`` version.

    .. tabs::

        .. code-tab:: py Reading files

            from onetl.file.format import XML
            from pyspark.sql import SparkSession

            # Create Spark session with XML package loaded
            maven_packages = XML.get_packages(spark_version="3.5.6")
            spark = (
                SparkSession.builder.appName("spark-app-name")
                .config("spark.jars.packages", ",".join(maven_packages))
                .getOrCreate()
            )

            xml = XML(rowTag="item", mode="PERMISSIVE")

        .. tab:: Writing files

            .. warning::

                Due to `bug <https://github.com/databricks/spark-xml/issues/664>`_ written files currently does not have ``.xml`` extension.

            .. code:: python

                # Create Spark session with XML package loaded
                spark = ...

                from onetl.file.format import XML

                xml = XML(rowTag="item", rootTag="data", compression="gzip")

    """

    name: ClassVar[str] = "xml"

    row_tag: str = Field(alias="rowTag")
    """
    XML tag that encloses each row in XML. Required.
    """

    rootTag: Optional[str] = None
    """
    XML tag that encloses content of all DataFrame. Default is ``ROWS``.

    .. note::

        Used only for writing files.
    """

    compression: Union[str, Literal["bzip2", "gzip", "lz4", "snappy"], None] = None
    """
    Compression codec. By default no compression is used.

    .. note::

        Used only for writing files.
    """

    timestampFormat: Optional[str] = None
    """
    Format string used for parsing or serializing timestamp values.
    By default, ISO 8601 format is used (``yyyy-MM-ddTHH:mm:ss.SSSZ``).
    """

    dateFormat: Optional[str] = None
    """
    Format string used for parsing or serializing date values.
    By default, ISO 8601 format is used (``yyyy-MM-dd``).
    """

    timezone: Optional[str] = None
    """
    Allows to override timezone used for parsing or serializing date and timestamp values.
    By default, ``spark.sql.session.timeZone`` is used.
    """

    nullValue: Optional[str] = None
    """
    String value used to represent null. Default is ``null`` string.
    """

    ignoreSurroundingSpaces: Optional[bool] = None
    """
    If ``True``, trim surrounding spaces while parsing values. Default ``false``.

    .. note::

        Used only for reading files or by :obj:`~parse_column` function.
    """

    mode: Optional[Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"]] = None
    """
    How to handle parsing errors:
      * ``PERMISSIVE`` - set field value as ``null``, move raw data to :obj:`~columnNameOfCorruptRecord` column.
      * ``DROPMALFORMED`` - skip the malformed row.
      * ``FAILFAST`` - throw an error immediately.

    Default is ``PERMISSIVE``.

    .. note::

        Used only for reading files or by :obj:`~parse_column` function.
    """

    columnNameOfCorruptRecord: Optional[str] = None
    """
    Name of DataFrame column there corrupted row is stored with ``mode=PERMISSIVE``.

    .. warning::

        If DataFrame schema is provided, this column should be added to schema explicitly:

        .. code:: python

            from onetl.connection import SparkLocalFS
            from onetl.file import FileDFReader
            from onetl.file.format import XML

            from pyspark.sql.types import StructType, StructField, TImestampType, StringType

            spark = ...
            schema = StructType(
                [
                    StructField("my_field", TimestampType()),
                    StructField("_corrupt_record", StringType()),  # <-- important
                ]
            )
            xml = XML(rowTag="item", columnNameOfCorruptRecord="_corrupt_record")

            reader = FileDFReader(
                connection=connection,
                format=xml,
                df_schema=schema,  # < ---
            )
            df = reader.run(["/some/file.xml"])

    .. note::

        Used only for reading files or by :obj:`~parse_column` function.
    """

    inferSchema: Optional[bool] = None
    """
    If ``True``, try to infer the input schema by reading a sample of the file (see :obj:`~samplingRatio`).
    Default ``False`` which means that all parsed columns will be ``StringType()``.

    .. note::

        Used only for reading files. Ignored by :obj:`~parse_column` function.
    """

    samplingRatio: Optional[float] = Field(default=None, ge=0, le=1)
    """
    For ``inferSchema=True``, read the specified fraction of rows to infer the schema.
    Default ``1``.

    .. note::

        Used only for reading files. Ignored by :obj:`~parse_column` function.
    """

    charset: Optional[str] = None
    """
    File encoding. Default is ``UTF-8``

    .. note::

        Used only for reading files or by :obj:`~parse_column` function.
    """

    valueTag: Optional[str] = None
    """
    Value used to replace missing values while parsing attributes like ``<sometag someattr>``.
    Default ``_VALUE``.

    .. note::

        Used only for reading files or by :obj:`~parse_column` function.
    """

    attributePrefix: Optional[str] = None
    """
    While parsing tags containing attributes like ``<sometag attr="value">``, attributes are stored as
    DataFrame schema columns with specified prefix, e.g. ``_attr``.
    Default ``_``.

    .. note::

        Used only for reading files or by :obj:`~parse_column` function.
    """

    excludeAttribute: Optional[bool] = None
    """
    If ``True``, exclude attributes while parsing tags like ``<sometag attr="value">``.
    Default ``false``.

    .. note::

        Used only for reading files or by :obj:`~parse_column` function.
    """

    wildcardColName: Optional[str] = None
    """
    Name of column or columns which should be preserved as raw XML string, and not parsed.

    .. warning::

        If DataFrame schema is provided, this column should be added to schema explicitly.
        See :obj:`~columnNameOfCorruptRecord` example.

    .. note::

        Used only for reading files or by :obj:`~parse_column` function.
    """

    ignoreNamespace: Optional[bool] = None
    """
    If ``True``, all namespaces like ``<ns:tag>`` will be ignored and treated as just ``<tag>``.
    Default ``False``.

    .. note::

        Used only for reading files or by :obj:`~parse_column` function.
    """

    rowValidationXSDPath: Optional[str] = None
    """
    Path to XSD file which should be used to validate each row.
    If row does not match XSD, it will be treated as error, behavior depends on :obj:`~mode` value.

    Default is no validation.

    .. note::

        If Spark session is created with ``master=yarn`` or ``master=k8s``, XSD
        file should be accessible from all Spark nodes. This can be achieved by calling:

        .. code:: python

            spark.addFile("/path/to/file.xsd")

        And then by passing ``rowValidationXSDPath=file.xsd`` (relative path).

    .. note::

        Used only for reading files or by :obj:`~parse_column` function.
    """

    declaration: Optional[str] = None
    """
    Content of `<?XML ... ?>` declaration.
    Default is ``version="1.0" encoding="UTF-8" standalone="yes"``.

    .. note::

        Used only for writing files.
    """

    arrayElementName: Optional[str] = None
    """
    If DataFrame column is ``ArrayType``, its content will be written to XML
    inside ``<arrayElementName>...</arrayElementName>`` tag.
    Default is ``item``.

    .. note::

        Used only for writing files.
    """

    class Config:
        known_options = frozenset()
        prohibited_options = frozenset(("path",))  # filled by FileDFReader/FileDFWriter
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

            XML.get_packages(spark_version="3.5.6")
            XML.get_packages(spark_version="3.5.6", scala_version="2.12")
            XML.get_packages(
                spark_version="3.5.6",
                scala_version="2.12",
                package_version="0.18.0",
            )

        """
        spark_ver = Version(spark_version)
        if spark_ver.major >= 4:
            # since Spark 4.0, XML is bundled with Spark
            return []

        if package_version:
            version = Version(package_version).min_digits(3)
            if version < Version("0.14"):
                raise ValueError(f"Package version must be above 0.13, got {version}")
            log.warning("Passed custom package version %r, it is not guaranteed to be supported", package_version)
        else:
            version = Version("0.18.0")

        scala_ver = Version(scala_version).min_digits(2) if scala_version else get_default_scala_version(spark_ver)
        return [f"com.databricks:spark-xml_{scala_ver.format('{0}.{1}')}:{version}"]

    @slot
    def check_if_supported(self, spark: SparkSession) -> None:
        version = get_spark_version(spark)
        if version.major >= 4:
            # since Spark 4.0, XML is bundled with Spark
            return

        java_class = "com.databricks.spark.xml.XmlReader"
        try:
            try_import_java_class(spark, java_class)
        except Exception as e:
            spark_version = get_spark_version(spark).format("{0}.{1}")
            msg = MISSING_JVM_CLASS_MSG.format(
                java_class=java_class,
                package_source=self.__class__.__name__,
                args=f"spark_version='{spark_version}'",
            )
            raise ValueError(msg) from e

    def parse_column(self, column: str | Column, schema: StructType) -> Column:
        """
        Parses an XML string column into a structured Spark SQL column using the ``from_xml`` function
        provided by the `Databricks Spark XML library <https://github.com/databricks/spark-xml#pyspark-notes>`_
        based on the provided schema.

        .. note::

            This method assumes that the ``spark-xml`` package is installed: :obj:`~get_packages`.

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
                                        StructField("title", StringType(), nullable=True),
                                        StructField("author", StringType(), nullable=True),
                                    ],
                                ),
                            ),
                            nullable=True,
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
        self._check_unsupported_serialization_options()

        from pyspark.sql.functions import col

        if isinstance(column, Column):
            column_name, column = column._jc.toString(), column.cast("string")  # noqa: WPS437
        else:
            column_name, column = column, col(column).cast("string")

        options = self.dict(by_alias=True, exclude_none=True)
        version = get_spark_version(spark)
        if version.major >= 4:
            from pyspark.sql.functions import from_xml  # noqa: WPS450

            return from_xml(column, schema, stringify(options)).alias(column_name)

        from pyspark.sql.column import _to_java_column  # noqa: WPS450

        java_column = _to_java_column(column)
        java_schema = spark._jsparkSession.parseDataType(schema.json())  # noqa: WPS437
        scala_options = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(  # noqa: WPS219, WPS437
            stringify(options),
        )
        jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(  # noqa: WPS219, WPS437
            java_column,
            java_schema,
            scala_options,
        )
        return Column(jc).alias(column_name)

    def _check_unsupported_serialization_options(self):
        current_options = self.dict(by_alias=True, exclude_none=True)
        unsupported_options = current_options.keys() & PARSE_COLUMN_UNSUPPORTED_OPTIONS
        if unsupported_options:
            warnings.warn(
                f"Options `{sorted(unsupported_options)}` are set but not supported in `XML.parse_column`.",
                UserWarning,
                stacklevel=2,
            )

    def __repr__(self):
        options_dict = self.dict(by_alias=True, exclude_none=True)
        options_dict = dict(sorted(options_dict.items()))
        options_kwargs = ", ".join(f"{k}={v!r}" for k, v in options_dict.items())
        return f"{self.__class__.__name__}({options_kwargs})"
