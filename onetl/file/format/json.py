# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from typing_extensions import Literal

from onetl._util.spark import stringify
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

    .. versionadded:: 0.9.0

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

        .. versionadded:: 0.11.0

        Parameters
        ----------
        column : str | Column
            The name of the column or the column object containing JSON strings/bytes to parse.

        schema : StructType | ArrayType | MapType
            The schema to apply when parsing the JSON data. This defines the structure of the output DataFrame column.

        Returns
        -------
        Column with deserialized data, with the same structure as the provided schema. Column name is the same as input column.

        Examples
        --------

        >>> from pyspark.sql.types import StructType, StructField, IntegerType, StringType
        >>> from pyspark.sql.functions import decode
        >>> from onetl.file.format import JSON
        >>> df.show()
        +----+--------------------+----------+---------+------+-----------------------+-------------+
        |key |value               |topic     |partition|offset|timestamp              |timestampType|
        +----+--------------------+----------+---------+------+-----------------------+-------------+
        |[31]|[7B 22 6E 61 6D 6...|topicJSON |0        |0     |2024-04-24 16:51:11.739|0            |
        |[32]|[7B 22 6E 61 6D 6...|topicJSON |0        |1     |2024-04-24 16:51:11.749|0            |
        +----+--------------------+----------+---------+------+-----------------------+-------------+
        >>> df.printSchema()
        root
        |-- key: binary (nullable = true)
        |-- value: binary (nullable = true)
        |-- topic: string (nullable = true)
        |-- partition: integer (nullable = true)
        |-- offset: integer (nullable = true)
        |-- timestamp: timestamp (nullable = true)
        |-- timestampType: integer (nullable = true)
        >>> json = JSON()
        >>> json_schema = StructType(
        ...     [
        ...         StructField("name", StringType(), nullable=True),
        ...         StructField("age", IntegerType(), nullable=True),
        ...     ],
        ... )
        >>> parsed_df = df.select(decode("key", "UTF-8").alias("key"), json.parse_column("value", json_schema))
        >>> parsed_df.show()
        +---+-----------+
        |key|value      |
        +---+-----------+
        |1  |{Alice, 20}|
        |2  |  {Bob, 25}|
        +---+-----------+
        >>> parsed_df.printSchema()
        root
        |-- key: string (nullable = true)
        |-- value: struct (nullable = true)
        |    |-- name: string (nullable = true)
        |    |-- age: integer (nullable = true)
        """
        from pyspark.sql import Column, SparkSession  # noqa:  WPS442
        from pyspark.sql.functions import col, from_json

        self.check_if_supported(SparkSession._instantiatedSession)  # noqa:  WPS437

        if isinstance(column, Column):
            column_name, column = column._jc.toString(), column.cast("string")  # noqa:  WPS437
        else:
            column_name, column = column, col(column).cast("string")

        options = stringify(self.dict(by_alias=True))
        return from_json(column, schema, options).alias(column_name)

    def serialize_column(self, column: str | Column) -> Column:
        """
        Serializes a structured Spark SQL column into a JSON string column using Spark's
        `to_json <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html>`_ function.

        .. versionadded:: 0.11.0

        Parameters
        ----------
        column : str | Column
            The name of the column or the column object containing the data to serialize to JSON format.

        Returns
        -------
        Column with string JSON data. Column name is the same as input column.

        Examples
        --------

        >>> from pyspark.sql.functions import decode
        >>> from onetl.file.format import JSON
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
        >>> # serializing data into JSON format
        >>> json = JSON()
        >>> serialized_df = df.select("key", json.serialize_column("value"))
        >>> serialized_df.show(truncate=False)
        +---+-------------------------+
        |key|value                    |
        +---+-------------------------+
        |  1|{"name":"Alice","age":20}|
        |  2|{"name":"Bob","age":25}  |
        +---+-------------------------+
        >>> serialized_df.printSchema()
        root
        |-- key: string (nullable = true)
        |-- value: string (nullable = true)
        """
        from pyspark.sql import Column, SparkSession  # noqa:  WPS442
        from pyspark.sql.functions import col, to_json

        self.check_if_supported(SparkSession._instantiatedSession)  # noqa:  WPS437

        if isinstance(column, Column):
            column_name = column._jc.toString()  # noqa:  WPS437
        else:
            column_name, column = column, col(column)

        options = stringify(self.dict(by_alias=True))
        return to_json(column, options).alias(column_name)
