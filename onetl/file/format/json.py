# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import warnings
from typing import TYPE_CHECKING, ClassVar, Literal, cast

from pydantic import ConfigDict, Field

from onetl._util.spark import stringify
from onetl.file.format.file_format import ReadOnlyFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import Column, SparkSession
    from pyspark.sql.types import ArrayType, MapType, StructType

PARSE_COLUMN_UNSUPPORTED_OPTIONS = {
    "encoding",
    "lineSep",
    "samplingRatio",
    "primitivesAsString",
    "prefersDecimal",
    "dropFieldIfAllNull",
}


@support_hooks
class JSON(ReadOnlyFileFormat):
    """
    JSON file format. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](/hooks/)

    Based on [Spark JSON](https://spark.apache.org/docs/latest/sql-data-sources-json.html) file format.

    Supports reading (but **NOT** writing) files with `.json` extension with content like:

    ```json title="example.json"
    [
        {"key": "value1"},
        {"key": "value2"}
    ]
    ```
    !!! success "Added in 0.9.0"

    Examples
    --------

    !!! note

        You can pass any option mentioned in
        [official documentation](https://spark.apache.org/docs/latest/sql-data-sources-json.html).
        **Option names should be in** `camelCase`!

        The set of supported options depends on Spark version.

    Reading files:

    ```python
    from onetl.file.format import JSON

    json = JSON(encoding="UTF-8")
    ```
    Writing files:

    !!! warning

        Not supported. Use [JSONLine][onetl.file.format.jsonline.JSONLine].

    """

    name: ClassVar[str] = "json"

    multiLine: Literal[True] = True

    encoding: str | None = None
    """
    Encoding of the JSON file.
    Default `UTF-8`.

    !!! note

        Used only for reading and writing files.

        Ignored by [parse_column][] and [serialize_column][] methods.
    """

    lineSep: str | None = None
    """
    Character used to separate lines in the JSON file.

    Defaults:
      * Try to detect for reading (`\\r\\n`, `\\r`, `\\n`)
      * `\\n` for writing

    !!! note

        Used only for reading and writing files.

        Ignored by [parse_column][] and [serialize_column][] methods,
        as they handle each DataFrame row separately.
    """

    allowComments: bool | None = None
    """
    If `True`, add support for C/C++/Java style comments (`//`, `/* */`).
    Default `False`, meaning that JSON files should not contain comments.

    !!! note

        Used only for reading files and [parse_column][] method.
    """

    allowUnquotedFieldNames: bool | None = None
    """
    If `True`, allow JSON object field names without quotes (JavaScript-style).
    Default `False`.

    !!! note

        Used only for reading files and [parse_column][] method.
    """

    allowSingleQuotes: bool | None = None
    """
    If `True`, allow JSON object field names to be wrapped with single quotes (`'`).
    Default `True`.

    !!! note

        Used only for reading files and [parse_column][] method.
    """

    allowNumericLeadingZeros: bool | None = None
    """
    If `True`, allow leading zeros in numbers (e.g. `00012`).
    Default `False`.

    !!! note

        Used only for reading files and [parse_column][] method.
    """

    allowNonNumericNumbers: bool | None = None
    """
    If `True`, allow numbers to contain non-numeric characters, like:
      * scientific notation (e.g. `12e10`).
      * positive infinity floating point value (`Infinity`, `+Infinity`, `+INF`).
      * negative infinity floating point value (`-Infinity`, `-INF`).
      * Not-a-Number floating point value (`NaN`).

    Default `True`.

    !!! note

        Used only for reading files and [parse_column][] method.
    """

    allowBackslashEscapingAnyCharacter: bool | None = None
    """
    If `True`, prefix `\\` can escape any character.
    Default `False`.

    !!! note

        Used only for reading files and [parse_column][] method.
    """

    allowUnquotedControlChars: bool | None = None
    """
    If `True`, allow unquoted control characters (ASCII values 0-31) in strings without escaping them with `\\`.
    Default `False`.

    !!! note

        Used only for reading files and [parse_column][] method.
    """

    mode: Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"] | None = None
    """
    How to handle parsing errors:
      * `PERMISSIVE` - set field value as `null`, move raw data to [columnNameOfCorruptRecord][] column.
      * `DROPMALFORMED` - skip the malformed row.
      * `FAILFAST` - throw an error immediately.

    Default is `PERMISSIVE`.

    !!! note

        Used only for reading files and [parse_column][] method.
    """

    columnNameOfCorruptRecord: str | None = Field(default=None, min_length=1)
    """
    Name of column to put corrupt records in.
    Default is `_corrupt_record`.

    !!! warning

        If DataFrame schema is provided, this column should be added to schema explicitly:

        ```python
        from onetl.connection import SparkLocalFS
        from onetl.file import FileDFReader
        from onetl.file.format import JSON

        from pyspark.sql.types import StructType, StructField, TimestampType, StringType

        spark = ...

        schema = StructType(
            [
                StructField("my_field", TimestampType()),
                StructField("_corrupt_record", StringType()),  # <-- important
            ]
        )

        json = JSON(mode="PERMISSIVE", columnNameOfCorruptRecord="_corrupt_record")

        reader = FileDFReader(
            connection=connection,
            format=json,
            df_schema=schema,  # < ---
        )
        df = reader.run(["/some/file.json"])
        ```
    !!! note

        Used only for reading files and [parse_column][] method.
    """

    samplingRatio: float | None = Field(default=None, ge=0, le=1)
    """
    While inferring schema, read the specified fraction of file rows.
    Default `1`.

    !!! note

        Used only for reading files. Ignored by [parse_column][] function.
    """

    primitivesAsString: bool | None = None
    """
    If `True`, infer all primitive types (string, integer, float, boolean) as strings.
    Default `False`.

    !!! note

        Used only for reading files. Ignored by [parse_column][] method.
    """

    prefersDecimal: bool | None = None
    """
    If `True`, infer all floating-point values as `Decimal`.
    Default `False`.

    !!! note

        Used only for reading files. Ignored by [parse_column][] method.
    """

    dropFieldIfAllNull: bool | None = None
    """
    If `True` and inferred column is always null or empty array, exclude if from DataFrame schema.
    Default `False`.

    !!! note

        Used only for reading files. Ignored by [parse_column][] method.
    """

    dateFormat: str | None = Field(default=None, min_length=1)
    """
    String format for `DateType()` representation.
    Default is `yyyy-MM-dd`.
    """

    timestampFormat: str | None = Field(default=None, min_length=1)
    """
    String format for `TimestampType()` representation.
    Default is `yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]`.
    """

    timestampNTZFormat: str | None = Field(default=None, min_length=1)
    """
    String format for `TimestampNTZType()` representation.
    Default is `yyyy-MM-dd'T'HH:mm:ss[.SSS]`.

    !!! note

        Added in Spark 3.2.0
    """

    timezone: str | None = Field(default=None, min_length=1, alias="timeZone")
    """
    Allows to override timezone used for parsing or serializing date and timestamp values.
    By default, `spark.sql.session.timeZone` is used.
    """

    locale: str | None = Field(default=None, min_length=1)
    """
    Locale name used to parse dates and timestamps.
    Default is `en-US`.

    !!! note

        Used only for reading files and [parse_column][] method.
    """
    model_config = ConfigDict(extra="allow", known_options=[])  # type: ignore[typeddict-unknown-key]

    @slot
    def check_if_supported(self, spark: "SparkSession") -> None:
        # always available
        pass

    def parse_column(self, column: "str | Column", schema: "StructType | ArrayType | MapType") -> "Column":
        """
        Parses a JSON string column to a structured Spark SQL column using Spark's
        [from_json](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html)
        function, based on the provided schema.

        !!! success "Added in 0.11.0"

        Parameters
        ----------
        column : str | pyspark.sql.Column
            The name of the column or the column object containing JSON strings/bytes to parse.

        schema : pyspark.sql.types.StructType | pyspark.sql.types.ArrayType | pyspark.sql.types.MapType
            The schema to apply when parsing the JSON data.
            This defines the structure of the output DataFrame column.

        Returns
        -------
        pyspark.sql.Column
            Column with deserialized data, with the same structure as the provided schema.
            Column name is the same as input column.

        Examples
        --------

        ```python
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
        ```
        """
        from pyspark.sql import Column, SparkSession
        from pyspark.sql.functions import col, from_json

        spark = cast("SparkSession", SparkSession._instantiatedSession)  # noqa: SLF001
        self.check_if_supported(spark)
        self._check_unsupported_serialization_options()

        if isinstance(column, Column):
            column_name, column = column._jc.toString(), column.cast("string")  # noqa: SLF001
        else:
            column_name, column = column, col(column).cast("string")

        options = stringify(self.model_dump(by_alias=True, exclude_none=True))
        return from_json(column, schema, options).alias(column_name)  # type: ignore[arg-type]

    def serialize_column(self, column: "str | Column") -> "Column":
        """
        Serializes a structured Spark SQL column into a JSON string column using Spark's
        [to_json](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.to_json.html) function.

        !!! success "Added in 0.11.0"

        Parameters
        ----------
        column : str | pyspark.sql.Column
            The name of the column or the column object containing the data to serialize to JSON format.

        Returns
        -------
        pyspark.sql.Column
            Column with string JSON data. Column name is the same as input column.

        Examples
        --------

        ```python
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
        ```
        """  # noqa: E501
        from pyspark.sql import Column, SparkSession
        from pyspark.sql.functions import col, to_json

        spark = cast("SparkSession", SparkSession._instantiatedSession)  # noqa: SLF001
        self.check_if_supported(spark)
        self._check_unsupported_serialization_options()

        if isinstance(column, Column):
            column_name = column._jc.toString()  # noqa: SLF001
        else:
            column_name, column = column, col(column)

        options = stringify(self.model_dump(by_alias=True, exclude_none=True))
        return to_json(column, options).alias(column_name)

    def _check_unsupported_serialization_options(self):
        current_options = self.model_dump(by_alias=True, exclude_none=True)
        unsupported_options = current_options.keys() & PARSE_COLUMN_UNSUPPORTED_OPTIONS
        if unsupported_options:
            warnings.warn(
                f"Options `{sorted(unsupported_options)}` are set but not supported "
                f"in `JSON.parse_column` or `JSON.serialize_column`.",
                UserWarning,
                stacklevel=2,
            )

    def __repr__(self):
        options_dict = self.model_dump(by_alias=True, exclude_none=True, exclude={"multiLine"})
        options_dict = dict(sorted(options_dict.items()))
        options_kwargs = ", ".join(f"{k}={v!r}" for k, v in options_dict.items())
        return f"{self.__class__.__name__}({options_kwargs})"
