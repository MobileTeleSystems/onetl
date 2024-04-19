# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from typing_extensions import Literal

from onetl.file.format.file_format import ReadOnlyFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import Column, SparkSession
    from pyspark.sql.types import ArrayType, MapType, StructType


READ_WRITE_OPTIONS = frozenset(
    (
        "dateFormat",
        "enableDateTimeParsingFallback",
        "timestampFormat",
        "timestampNTZFormat",
        "timeZone",
    ),
)

READ_OPTIONS = frozenset(
    (
        "allowBackslashEscapingAnyCharacter",
        "allowComments",
        "allowNonNumericNumbers",
        "allowNumericLeadingZeros",
        "allowSingleQuotes",
        "allowUnquotedControlChars",
        "allowUnquotedFieldNames",
        "columnNameOfCorruptRecord",
        "dropFieldIfAllNull",
        "locale",
        "mode",
        "prefersDecimal",
        "primitivesAsString",
        "samplingRatio",
    ),
)

WRITE_OPTIONS = frozenset(
    (
        "compression",
        "ignoreNullFields",
    ),
)


@support_hooks
class JSON(ReadOnlyFileFormat):
    """
    JSON file format. |support_hooks|

    Based on `Spark JSON <https://spark.apache.org/docs/latest/sql-data-sources-json.html>`_ file format.

    Supports reading (but **NOT** writing) files with ``.json`` extension with content like:

    .. code-block:: json
        :caption: example.json

        [
            {"key": "value1"},
            {"key": "value2"}
        ]

    .. warning::

        For writing prefer using :obj:`JSONLine <onetl.file.format.jsonline.JSONLine>`.

    .. note ::

        You can pass any option to the constructor, even if it is not mentioned in this documentation.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version. See link above.

    Examples
    --------

    Describe options how to read from/write to JSON file with specific options:

    .. code:: python

        json = JSON(encoding="utf-8", compression="gzip")

    """

    name: ClassVar[str] = "json"

    multiLine: Literal[True] = True  # noqa: N815
    encoding: str = "utf-8"
    lineSep: str = "\n"  # noqa: N815

    class Config:
        known_options = READ_WRITE_OPTIONS | READ_OPTIONS | WRITE_OPTIONS
        extra = "allow"

    @slot
    def check_if_supported(self, spark: SparkSession) -> None:
        # always available
        pass

    def parse_column(self, column: str | Column, schema: StructType | ArrayType | MapType) -> Column:
        """
        Parses a JSON string column to a structured Spark SQL column using Spark's `from_json <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html>`_ function, based on the provided schema.

        Parameters
        ----------
        column : str | Column
            The name of the column or the Column object containing JSON strings to parse.

        schema : StructType | ArrayType | MapType
            The schema to apply when parsing the JSON data. This defines the structure of the output DataFrame column.

        Returns
        -------
        Column
            A new Column object with data parsed from JSON string to the specified structure.

        Examples
        --------
        .. code:: python

            from pyspark.sql import SparkSession
            from pyspark.sql.types import StructType, StructField, IntegerType, StringType

            spark = SparkSession.builder.appName("JSONParsingExample").getOrCreate()
            json = JSON()
            df = spark.createDataFrame([(1, '{"id":123, "name":"John"}')], ["id", "json_string"])
            schema = StructType(
                [StructField("id", IntegerType()), StructField("name", StringType())]
            )

            parsed_df = df.withColumn("parsed_json", json.parse_column("json_string", schema))
            parsed_df.show()
        """
        from pyspark.sql import Column, SparkSession  # noqa:  WPS442
        from pyspark.sql.functions import col, from_json

        self.check_if_supported(SparkSession._instantiatedSession)  # noqa:  WPS437

        if isinstance(column, Column):
            column_name, column = column._jc.toString(), column.cast("string")  # noqa:  WPS437
        else:
            column_name, column = column, col(column).cast("string")

        return from_json(column, schema, self.dict()).alias(column_name)

    def serialize_column(self, column: str | Column) -> Column:
        """
        Serializes a structured Spark SQL column into a JSON string column using Spark's `to_json <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html>`_ function.

        Parameters
        ----------
        column : str | Column
            The name of the column or the Column object containing the data to serialize to JSON.

        Returns
        -------
        Column
            A new Column object with data serialized from Spark SQL structures to JSON string.

        Examples
        --------
        .. code:: python

            from pyspark.sql import SparkSession
            from pyspark.sql.functions import struct

            spark = SparkSession.builder.appName("JSONSerializationExample").getOrCreate()
            json = JSON()
            df = spark.createDataFrame([(123, "John")], ["id", "name"])
            df = df.withColumn("combined", struct("id", "name"))

            serialized_df = df.withColumn("json_string", json.serialize_column("combined"))
            serialized_df.show()
        """
        from pyspark.sql import Column, SparkSession  # noqa:  WPS442
        from pyspark.sql.functions import col, to_json

        self.check_if_supported(SparkSession._instantiatedSession)  # noqa:  WPS437

        if isinstance(column, Column):
            column_name = column._jc.toString()  # noqa:  WPS437
        else:
            column_name, column = column, col(column)

        return to_json(column, self.dict()).alias(column_name)
