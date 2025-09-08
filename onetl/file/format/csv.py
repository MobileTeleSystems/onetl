# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, ClassVar, Optional, Union

from typing_extensions import Literal

try:
    from pydantic.v1 import Field
except (ImportError, AttributeError):
    from pydantic import Field  # type: ignore[no-redef, assignment]

from onetl._util.alias import avoid_alias
from onetl._util.spark import stringify
from onetl.file.format.file_format import ReadWriteFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import Column, SparkSession
    from pyspark.sql.types import StructType

PARSE_COLUMN_UNSUPPORTED_OPTIONS = {
    "header",
    "compression",
    "encoding",
    "inferSchema",
    "samplingRatio",
    "enforceSchema",
    "preferDate",
    "multiLine",
}


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

    .. versionadded:: 0.9.0

    Examples
    --------

    .. note ::

        You can pass any option mentioned in
        `official documentation <https://spark.apache.org/docs/latest/sql-data-sources-csv.html>`_.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version.

    .. tabs::

        .. code-tab:: py Reading files

            from onetl.file.format import CSV

            csv = CSV(header=True, inferSchema=True, mode="PERMISSIVE")

        .. code-tab:: py Writing files

            from onetl.file.format import CSV

            csv = CSV(header=True, compression="gzip")

    """

    name: ClassVar[str] = "csv"

    delimiter: str = Field(default=",", alias=avoid_alias("sep"))  # type: ignore[literal-required]
    """
    Character used to separate fields in CSV row.
    """

    header: Optional[bool] = None
    """
    If ``True``, the first row of the file is considered a header.
    Default ``False``.
    """

    quote: str = Field(default='"', max_length=1)
    """
    Character used to quote field values within CSV field.

    Empty string is considered as ``\\u0000`` (``NUL``) character.
    """

    quoteAll: Optional[bool] = None
    """
    If ``True``, all fields are quoted:

    .. code:: csv

        "some","field with \\"quote","123",""

    If ``False``, only quote fields containing :obj:`~quote` symbols.

    .. code:: csv

        any,"field with \\"quote",123,

    Default ``False``.

    .. note::

        Used only for writing files.
    """

    escape: str = Field(default="\\", max_length=1)
    """
    Character used to escape quotes within CSV field.

    Empty string is considered as ``\\u0000`` (``NUL``) character.
    """

    lineSep: Optional[str] = Field(default=None, min_length=1, max_length=2)
    """
    Character used to separate lines in the CSV file.

    Defaults:
      * Try to detect for reading (``\\r\\n``, ``\\r``, ``\\n``)
      * ``\\n`` for writing

    .. note::

        Used only for reading and writing files.
        Ignored by :obj:`~parse_column` method, as it expects that each DataFrame row will contain exactly one CSV line.
    """

    encoding: Optional[str] = Field(default=None, min_length=1)
    """
    Encoding of the CSV file.
    Default ``UTF-8``.

    .. note::

        Used only for reading and writing files. Ignored by :obj:`~parse_column` method.
    """

    compression: Union[str, Literal["none", "bzip2", "gzip", "lz4", "snappy", "deflate"], None] = None
    """
    Compression codec of the CSV file.
    Default ``none``.

    .. note::

        Used only for writing files. Ignored by :obj:`~parse_column` method.
    """

    inferSchema: Optional[bool] = None
    """
    If ``True``, try to infer the input schema by reading a sample of the file (see :obj:`~samplingRatio`).
    Default ``False`` which means that all parsed columns will be ``StringType()``.

    .. note::

        Used only for reading files, and only if user haven't provider explicit DataFrame schema.
        Ignored by :obj:`~parse_column` function.
    """

    samplingRatio: Optional[float] = Field(default=None, ge=0, le=1)
    """
    For ``inferSchema=True``, read the specified fraction of rows to infer the schema.
    Default ``1``.

    .. note::

        Used only for reading files. Ignored by :obj:`~parse_column` function.
    """

    comment: Optional[str] = Field(default=None, max_length=1)
    """
    If set, all lines starting with specified character (e.g. ``#``) are considered a comment, and skipped.
    Default is not set, meaning that CSV lines should not contain comments.

    .. note::

        Used only for reading files and :obj:`~parse_column` method.
    """

    enforceSchema: Optional[bool] = None
    """
    If ``True``, inferred or user-provided schema has higher priority than CSV file headers.
    This means that all input files should have the same structure.

    If ``False``, CSV headers are used as a primary source of information about column names and their position.

    Default ``True``.

    .. note::

        Used only for reading files. Ignored by :obj:`~parse_column` function.
    """

    escapeQuotes: Optional[bool] = None
    """
    If ``True``, escape quotes within CSV field.

    .. code:: csv

        any,field with \\"quote,123,

    If ``False``, wrap fields containing :obj:`~quote` symbols with quotes.

    .. code:: csv

        any,"field with ""quote ",123,

    Default ``True``.

    .. note::

        Used only for writing files.
    """

    unescapedQuoteHandling: Union[
        None,
        Literal[
            "STOP_AT_CLOSING_QUOTE",
            "BACK_TO_DELIMITER",
            "STOP_AT_DELIMITER",
            "SKIP_VALUE",
            "RAISE_ERROR",
        ],
    ] = None
    """
    Define how to handle unescaped quotes within CSV field.

    * ``STOP_AT_CLOSING_QUOTE`` - collect all characters until closing quote.
    * ``BACK_TO_DELIMITER`` - collect all characters until delimiter or line end.
    * ``STOP_AT_DELIMITER`` - collect all characters until delimiter or line end.
       If quotes are not closed, this may produce incorrect results (e.g. including delimiter inside field value).
    * ``SKIP_VALUE`` - skip field and consider it as :obj:`~nullValue`.
    * ``RAISE_ERROR`` - raise error immediately.

    Default ``STOP_AT_DELIMITER``.

    .. note::

        Used only for reading files and :obj:`~parse_column` method.
    """

    ignoreLeadingWhiteSpace: Optional[bool] = None
    """
    If ``True``, trim leading whitespaces in field value.

    Defaults:
      * ``True`` for reading.
      * ``False`` for writing.
    """

    ignoreTrailingWhiteSpace: Optional[bool] = None
    """
    If ``True``, trim trailing whitespaces in field value.

    Defaults:
      * ``True`` for reading.
      * ``False`` for writing.
    """

    emptyValue: Optional[str] = None
    """
    Value used for empty string fields.

    Defaults:
      * empty string for reading.
      * ``""`` for writing.
    """

    nullValue: Optional[str] = None
    """
    If set, this value will be converted to ``null``.
    Default is empty string.
    """

    nanValue: Optional[str] = Field(default=None)
    """
    If set, this string will be considered as Not-A-Number (NaN) value for ``FloatType()`` and ``DoubleType()``.
    Default is ``NaN``.

    .. note::

        Used only for reading files and :obj:`~parse_column` method.
    """

    positiveInf: Optional[str] = Field(default=None, min_length=1)
    """
    If set, this string will be considered as positive infinity value for ``FloatType()`` and ``DoubleType()``.
    Default is ``Inf``.

    .. note::

        Used only for reading files and :obj:`~parse_column` method.
    """

    negativeInf: Optional[str] = Field(default=None, min_length=1)
    """
    If set, this string will be considered as negative infinity value for ``FloatType()`` and ``DoubleType()``.
    Default is ``-Inf``.

    .. note::

        Used only for reading files and :obj:`~parse_column` method.
    """

    preferDate: Optional[bool] = None
    """
    If ``True`` and ``inferSchema=True`` and column does match :obj:`~dateFormat`, consider it as ``DateType()``.
    For columns matching both :obj:`~timestampFormat` and :obj:`~dateFormat`, consider it as ``TimestampType()``.

    If ``False``, date and timestamp columns will be considered as ``StringType()``.

    Default ``True``.

    .. note::

        Used only for reading files. Ignored by :obj:`~parse_column` function.
    """

    dateFormat: Optional[str] = Field(default=None, min_length=1)
    """
    String format for ``DateType()`` representation.
    Default is ``yyyy-MM-dd``.
    """

    timestampFormat: Optional[str] = Field(default=None, min_length=1)
    """
    String format for `TimestampType()`` representation.
    Default is ``yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]``.
    """

    timestampNTZFormat: Optional[str] = Field(default=None, min_length=1)
    """
    String format for `TimestampNTZType()`` representation.
    Default is ``yyyy-MM-dd'T'HH:mm:ss[.SSS]``.

    .. note::

        Added in Spark 3.2.0
    """

    locale: Optional[str] = Field(default=None, min_length=1)
    """
    Locale name used to parse dates and timestamps.
    Default is ``en-US``

    ..  note::

        Used only for reading files and :obj:`~parse_column` method.
    """

    maxCharsPerColumn: Optional[int] = None
    """
    Maximum number of characters to read per column.
    Default is ``-1``, which means no limit.

    .. note::

        Used only for reading files and :obj:`~parse_column` method.
    """

    mode: Optional[Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"]] = None
    """
    How to handle parsing errors:
      * ``PERMISSIVE`` - set field value as ``null``, move raw data to :obj:`~columnNameOfCorruptRecord` column.
      * ``DROPMALFORMED`` - skip the malformed row.
      * ``FAILFAST`` - throw an error immediately.

    Default is ``PERMISSIVE``.

    .. note::

        Used only for reading files and :obj:`~parse_column` method.
    """

    columnNameOfCorruptRecord: Optional[str] = Field(default=None, min_length=1)
    """
    Name of column to put corrupt records in.
    Default is ``_corrupt_record``.

    .. warning::

        If DataFrame schema is provided, this column should be added to schema explicitly:

        .. code:: python

            from onetl.connection import SparkLocalFS
            from onetl.file import FileDFReader
            from onetl.file.format import CSV

            from pyspark.sql.types import StructType, StructField, TimestampType, StringType

            spark = ...

            schema = StructType(
                [
                    StructField("my_field", TimestampType()),
                    StructField("_corrupt_record", StringType()),  # <-- important
                ]
            )

            csv = CSV(mode="PERMISSIVE", columnNameOfCorruptRecord="_corrupt_record")

            reader = FileDFReader(
                connection=connection,
                format=csv,
                df_schema=schema,  # < ---
            )
            df = reader.run(["/some/file.csv"])

    .. note::

        Used only for reading files and :obj:`~parse_column` method.
    """

    multiLine: Optional[bool] = None
    """
    If ``True``, fields may contain line separators.
    If ``False``, the input is expected to have one record per file.

    Default is ``True``.

    .. note::

        Used only for reading files.
        Ignored by :obj:`~parse_column` method, as it expects that each DataFrame row will contain exactly one CSV line.
    """

    charToEscapeQuoteEscaping: Optional[str] = Field(default=None, max_length=1)
    """
    If CSV field value contains :obj:`~escape` character, it should be escaped as well.
    For example, if ``escape="\\"``, when line:

    .. code:: csv

        "some \\" quoted value",other
        "some \\\\ backslashed value",another

    will be parsed as:

    .. code:: python

        [
            ('some " quoted value', "other"),
            ("some \\ backslashed value", "another"),
        ]

    And vice-versa, for writing CSV rows to file.

    Default is same as :obj:`~escape`.
    """

    class Config:
        known_options = frozenset()
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
        self._check_unsupported_serialization_options()

        from pyspark.sql.functions import col, from_csv

        if isinstance(column, Column):
            column_name = column._jc.toString()  # noqa: WPS437
        else:
            column_name, column = column, col(column).cast("string")

        schema_string = schema.simpleString()
        options = stringify(self.dict(by_alias=True, exclude_none=True))
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
        self._check_unsupported_serialization_options()

        from pyspark.sql.functions import col, to_csv

        if isinstance(column, Column):
            column_name = column._jc.toString()  # noqa: WPS437
        else:
            column_name, column = column, col(column)

        options = stringify(self.dict(by_alias=True, exclude_none=True))
        return to_csv(column, options).alias(column_name)

    def _check_unsupported_serialization_options(self):
        current_options = self.dict(by_alias=True, exclude_none=True)
        unsupported_options = current_options.keys() & PARSE_COLUMN_UNSUPPORTED_OPTIONS
        if unsupported_options:
            warnings.warn(
                f"Options `{sorted(unsupported_options)}` are set but not supported in `CSV.parse_column` or `CSV.serialize_column`.",
                UserWarning,
                stacklevel=2,
            )

    def __repr__(self):
        options_dict = self.dict(by_alias=True, exclude_none=True)
        options_dict = dict(sorted(options_dict.items()))
        options_kwargs = ", ".join(f"{k}={v!r}" for k, v in options_dict.items())
        return f"{self.__class__.__name__}({options_kwargs})"
