# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, ClassVar

try:
    from pydantic.v1 import Field
except (ImportError, AttributeError):
    from pydantic import Field  # type: ignore[no-redef, assignment]

from onetl._util.spark import get_spark_version, stringify
from onetl.file.format.file_format import ReadWriteFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import Column, SparkSession
    from pyspark.sql.types import StructType


READ_WRITE_OPTIONS = frozenset(
    (
        "charToEscapeQuoteEscaping",
        "dateFormat",
        "emptyValue",
        "ignoreLeadingWhiteSpace",
        "ignoreTrailingWhiteSpace",
        "nullValue",
        "timestampFormat",
        "timestampNTZFormat",
    ),
)

READ_OPTIONS = frozenset(
    (
        "columnNameOfCorruptRecord",
        "comment",
        "enableDateTimeParsingFallback",
        "enforceSchema",
        "inferSchema",
        "locale",
        "maxCharsPerColumn",
        "maxColumns",
        "mode",
        "multiLine",
        "nanValue",
        "negativeInf",
        "positiveInf",
        "preferDate",
        "samplingRatio",
        "unescapedQuoteHandling",
    ),
)

WRITE_OPTIONS = frozenset(
    (
        "compression",
        "escapeQuotes",
        "quoteAll",
    ),
)


@support_hooks
class CSV(ReadWriteFileFormat):
    """
    CSV file format. |support_hooks|

    Based on `Spark CSV <https://spark.apache.org/docs/latest/sql-data-sources-csv.html>`_ file format.

    Supports reading/writing files with ``.csv`` extension with content like:

    .. code-block:: csv
        :caption: example.csv

        "some","value"
        "another","value"

    .. note ::

        You can pass any option to the constructor, even if it is not mentioned in this documentation.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version. See link above.

    .. versionadded:: 0.9.0

    Examples
    --------

    Describe options how to read from/write to CSV file with specific options:

    .. code:: python

        csv = CSV(sep=",", encoding="utf-8", inferSchema=True, compression="gzip")

    """

    name: ClassVar[str] = "csv"
    delimiter: str = Field(default=",", alias="sep")
    encoding: str = "utf-8"
    quote: str = '"'
    escape: str = "\\"
    header: bool = False
    lineSep: str = "\n"  # noqa: N815

    class Config:
        known_options = READ_WRITE_OPTIONS | READ_OPTIONS | WRITE_OPTIONS
        extra = "allow"

    @slot
    @classmethod
    def check_if_supported(cls, spark: SparkSession) -> None:
        # always available
        pass

    def parse_column(self, column: str | Column, schema: StructType) -> Column:
        """
        Parses a CSV string column to a structured Spark SQL column using Spark's
        `from_csv <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_csv.html>`_ function, based on the provided schema.

        .. note::

            Can be used only with Spark 3.x+

        .. versionadded:: 0.11.0

        Parameters
        ----------
        column : str | Column
            The name of the column or the column object containing CSV strings/bytes to parse.

        schema : StructType
            The schema to apply when parsing the CSV data. This defines the structure of the output DataFrame column.

        Returns
        -------
        Column with deserialized data, with the same structure as the provided schema. Column name is the same as input column.

        Examples
        --------

        >>> from pyspark.sql.types import StructType, StructField, IntegerType, StringType
        >>> from onetl.file.format import CSV
        >>> df.show()
        +--+--------+
        |id|value   |
        +--+--------+
        |1 |Alice;20|
        |2 |Bob;25  |
        +--+--------+
        >>> df.printSchema()
        root
        |-- id: integer (nullable = true)
        |-- value: string (nullable = true)
        >>> csv = CSV(delimiter=";")
        >>> csv_schema = StructType(
        ...     [
        ...         StructField("name", StringType(), nullable=True),
        ...         StructField("age", IntegerType(), nullable=True),
        ...     ],
        ... )
        >>> parsed_df = df.select("id", csv.parse_column("value", csv_schema))
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
        self._check_spark_version_for_serialization(spark)
        self._check_unsupported_serialization_options()

        from pyspark.sql.functions import col, from_csv

        if isinstance(column, Column):
            column_name = column._jc.toString()  # noqa: WPS437
        else:
            column_name, column = column, col(column).cast("string")

        schema_string = schema.simpleString()
        options = stringify(self.dict(by_alias=True))
        return from_csv(column, schema_string, options).alias(column_name)

    def serialize_column(self, column: str | Column) -> Column:
        """
        Serializes a structured Spark SQL column into a CSV string column using Spark's
        `to_csv <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_csv.html>`_ function.

        .. note::

            Can be used only with Spark 3.x+

        .. versionadded:: 0.11.0

        Parameters
        ----------
        column : str | Column
            The name of the column or the Column object containing the data to serialize to CSV.

        Returns
        -------
        Column with string CSV data. Column name is the same as input column.

        Examples
        --------

        >>> from pyspark.sql.functions import decode
        >>> from onetl.file.format import CSV
        >>> df.show()
        +--+-----------+
        |id|value      |
        +--+-----------+
        |1 |{Alice, 20}|
        |2 |  {Bob, 25}|
        +--+-----------+
        >>> df.printSchema()
        root
        |-- id: integer (nullable = true)
        |-- value: struct (nullable = true)
        |    |-- name: string (nullable = true)
        |    |-- age: integer (nullable = true)
        >>> # serializing data into CSV format
        >>> csv = CSV(delimiter=";")
        >>> serialized_df = df.select("id", csv.serialize_column("value"))
        >>> serialized_df.show(truncate=False)
        +--+--------+
        |id|value   |
        +--+--------+
        |1 |Alice;20|
        |2 |Bob;25  |
        +--+--------+
        >>> serialized_df.printSchema()
        root
        |-- id: integer (nullable = true)
        |-- value: string (nullable = true)
        """

        from pyspark.sql import Column, SparkSession  # noqa: WPS442

        spark = SparkSession._instantiatedSession  # noqa: WPS437
        self.check_if_supported(spark)
        self._check_spark_version_for_serialization(spark)
        self._check_unsupported_serialization_options()

        from pyspark.sql.functions import col, to_csv

        if isinstance(column, Column):
            column_name = column._jc.toString()  # noqa: WPS437
        else:
            column_name, column = column, col(column)

        options = stringify(self.dict(by_alias=True))
        return to_csv(column, options).alias(column_name)

    def _check_spark_version_for_serialization(self, spark: SparkSession):
        spark_version = get_spark_version(spark)
        if spark_version.major < 3:
            class_name = self.__class__.__name__
            error_msg = (
                f"`{class_name}.parse_column` or `{class_name}.serialize_column` are available "
                f"only since Spark 3.x, but got {spark_version}"
            )
            raise ValueError(error_msg)

    def _check_unsupported_serialization_options(self):
        unsupported_options = ["header", "compression", "inferSchema"]
        current_options = self.dict()
        for option in unsupported_options:
            if current_options.get(option):
                warnings.warn(
                    f"Option `{option}` is set but not supported in `CSV.parse_column` or `CSV.serialize_column`. "
                    "This may lead to unexpected behavior.",
                    UserWarning,
                    stacklevel=2,
                )
