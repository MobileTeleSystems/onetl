# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING, ClassVar, Literal

from pydantic import ConfigDict, Field

from onetl.file.format.file_format import ReadWriteFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@support_hooks
class JSONLine(ReadWriteFileFormat):
    """
    JSONLine file format (each line of file contains a JSON object). [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

    Based on [Spark JSON](https://spark.apache.org/docs/latest/sql-data-sources-json.html) file format.

    Supports reading/writing files with `.json` extension with content like:

    ```json title="example.json"
    {"key": "value1"}
    {"key": "value2"}
    ```
    !!! success "Added in 0.9.0"

    Examples
    --------

    !!! note

        You can pass any option mentioned in
        [official documentation](https://spark.apache.org/docs/latest/sql-data-sources-json.html).
        **Option names should be in** `camelCase`!

        The set of supported options depends on Spark version.

    === "Reading files"

        ```python
        from onetl.file.format import JSONLine

        jsonline = JSONLine(encoding="UTF-8", mode="PERMISSIVE")
        ```

    === "Writing files"

        !!! warning

            Written files have extension `.json`, not `.jsonl` or `.jsonline`.

        ```python
        from onetl.file.format import JSONLine

        jsonline = JSONLine(encoding="UTF-8", compression="gzip")
        ```
    """

    name: ClassVar[str] = "json"

    multiLine: Literal[False] = False

    encoding: str | None = None
    """
    Encoding of the JSONLine files.
    Default `UTF-8`.
    """

    lineSep: str | None = None
    """
    Character used to separate lines in the JSONLine files.

    Defaults:
      * Try to detect for reading (`\\r\\n`, `\\r`, `\\n`)
      * `\\n` for writing.
    """

    compression: str | Literal["none", "bzip2", "gzip", "lz4", "snappy", "deflate"] | None = None
    """
    Compression codec of the JSONLine file.
    Default `none`.

    !!! note

        Used only for writing files.
    """

    ignoreNullFields: bool | None = None
    """
    If `True` and field value is `null`, don't add field into resulting object
    Default is value of `spark.sql.jsonGenerator.ignoreNullFields` (`True`).

    !!! note

        Used only for writing files.
    """

    allowComments: bool | None = None
    """
    If `True`, add support for C/C++/Java style comments (`//`, `/* */`).
    Default `False`, meaning that JSONLine files should not contain comments.

    !!! note

        Used only for reading files.
    """

    allowUnquotedFieldNames: bool | None = None
    """
    If `True`, allow JSON object field names without quotes (JavaScript-style).
    Default `False`.

    !!! note

        Used only for reading files.
    """

    allowSingleQuotes: bool | None = None
    """
    If `True`, allow JSON object field names to be wrapped with single quotes (`'`).
    Default `True`.

    !!! note

        Used only for reading files.
    """

    allowNumericLeadingZeros: bool | None = None
    """
    If `True`, allow leading zeros in numbers (e.g. `00012`).
    Default `False`.

    !!! note

        Used only for reading files.
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

        Used only for reading files.
    """

    allowBackslashEscapingAnyCharacter: bool | None = None
    """
    If `True`, prefix `\\` can escape any character.
    Default `False`.

    !!! note

        Used only for reading files.
    """

    allowUnquotedControlChars: bool | None = None
    """
    If `True`, allow unquoted control characters (ASCII values 0-31) in strings without escaping them with `\\`.
    Default `False`.

    !!! note

        Used only for reading files.
    """

    mode: Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"] | None = None
    """
    How to handle parsing errors:
      * `PERMISSIVE` - set field value as `null`, move raw data to [columnNameOfCorruptRecord][] column.
      * `DROPMALFORMED` - skip the malformed row.
      * `FAILFAST` - throw an error immediately.

    Default is `PERMISSIVE`.

    !!! note

        Used only for reading files.
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
        from onetl.file.format import JSONLine

        from pyspark.sql.types import StructType, StructField, TimestampType, StringType

        spark = ...

        schema = StructType(
            [
                StructField("my_field", TimestampType()),
                StructField("_corrupt_record", StringType()),  # <-- important
            ]
        )

        jsonline = JSONLine(mode="PERMISSIVE", columnNameOfCorruptRecord="_corrupt_record")

        reader = FileDFReader(
            connection=connection,
            format=jsonline,
            df_schema=schema,  # < ---
        )
        df = reader.run(["/some/file.jsonl"])
        ```
    !!! note

        Used only for reading files.
    """

    samplingRatio: float | None = Field(default=None, ge=0, le=1)
    """
    While inferring schema, read the specified fraction of file rows.
    Default `1`.

    !!! note

        Used only for reading files.
    """

    primitivesAsString: bool | None = None
    """
    If `True`, infer all primitive types (string, integer, float, boolean) as strings.
    Default `False`.

    !!! note

        Used only for reading files.
    """

    prefersDecimal: bool | None = None
    """
    If `True`, infer all floating-point values as `Decimal`.
    Default `False`.

    !!! note

        Used only for reading files.
    """

    dropFieldIfAllNull: bool | None = None
    """
    If `True` and inferred column is always null or empty array, exclude if from DataFrame schema.
    Default `False`.

    !!! note

        Used only for reading files.
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

        Used only for reading files.
    """
    model_config = ConfigDict(extra="allow", known_options=[])  # type: ignore[typeddict-unknown-key]

    @slot
    def check_if_supported(self, spark: "SparkSession") -> None:
        # always available
        pass

    def __repr__(self):
        options_dict = self.model_dump(by_alias=True, exclude_none=True, exclude={"multiLine"})
        options_dict = dict(sorted(options_dict.items()))
        options_kwargs = ", ".join(f"{k}={v!r}" for k, v in options_dict.items())
        return f"{self.__class__.__name__}({options_kwargs})"
