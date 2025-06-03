# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from typing_extensions import Literal

try:
    from pydantic.v1 import Field
except (ImportError, AttributeError):
    from pydantic import Field  # type: ignore[no-redef, assignment]

from onetl.file.format.file_format import ReadWriteFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@support_hooks
class JSONLine(ReadWriteFileFormat):
    """
    JSONLine file format (each line of file contains a JSON object). |support_hooks|

    Based on `Spark JSON <https://spark.apache.org/docs/latest/sql-data-sources-json.html>`_ file format.

    Supports reading/writing files with ``.json`` extension with content like:

    .. code-block:: json
        :caption: example.json

        {"key": "value1"}
        {"key": "value2"}

    .. versionadded:: 0.9.0

    Examples
    --------

    .. note ::

        You can pass any option mentioned in
        `official documentation <https://spark.apache.org/docs/latest/sql-data-sources-json.html>`_.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version.

    .. tabs::

        .. code-tab:: py Reading files

            from onetl.file.format import JSONLine

            jsonline = JSONLine(encoding="UTF-8", mode="PERMISSIVE")

        .. tab:: Writing files

            .. warning::

                Written files have extension ``.json``, not ``.jsonl`` or ``.jsonline``.

            .. code:: python

                from onetl.file.format import JSONLine

                jsonline = JSONLine(encoding="UTF-8", compression="gzip")
    """

    name: ClassVar[str] = "json"

    multiLine: Literal[False] = False

    encoding: Optional[str] = None
    """
    Encoding of the JSONLine files.
    Default ``UTF-8``.
    """

    lineSep: Optional[str] = None
    """
    Character used to separate lines in the JSONLine files.

    Defaults:
      * Try to detect for reading (``\\r\\n``, ``\\r``, ``\\n``)
      * ``\\n`` for writing.
    """

    compression: Union[str, Literal["none", "bzip2", "gzip", "lz4", "snappy", "deflate"], None] = None
    """
    Compression codec of the JSONLine file.
    Default ``none``.

    .. note::

        Used only for writing files.
    """

    ignoreNullFields: Optional[bool] = None
    """
    If ``True`` and field value is ``null``, don't add field into resulting object
    Default is value of ``spark.sql.jsonGenerator.ignoreNullFields`` (``True``).

    .. note::

        Used only for writing files.
    """

    allowComments: Optional[bool] = None
    """
    If ``True``, add support for C/C++/Java style comments (``//``, ``/* */``).
    Default ``False``, meaning that JSONLine files should not contain comments.

    .. note::

        Used only for reading files.
    """

    allowUnquotedFieldNames: Optional[bool] = None
    """
    If ``True``, allow JSON object field names without quotes (JavaScript-style).
    Default ``False``.

    .. note::

        Used only for reading files.
    """

    allowSingleQuotes: Optional[bool] = None
    """
    If ``True``, allow JSON object field names to be wrapped with single quotes (``'``).
    Default ``True``.

    .. note::

        Used only for reading files.
    """

    allowNumericLeadingZeros: Optional[bool] = None
    """
    If ``True``, allow leading zeros in numbers (e.g. ``00012``).
    Default ``False``.

    .. note::

        Used only for reading files.
    """

    allowNonNumericNumbers: Optional[bool] = None
    """
    If ``True``, allow numbers to contain non-numeric characters, like:
      * scientific notation (e.g. ``12e10``).
      * positive infinity floating point value (``Infinity``, ``+Infinity``, ``+INF``).
      * negative infinity floating point value (``-Infinity``, ``-INF``).
      * Not-a-Number floating point value (``NaN``).

    Default ``True``.

    .. note::

        Used only for reading files.
    """

    allowBackslashEscapingAnyCharacter: Optional[bool] = None
    """
    If ``True``, prefix ``\\`` can escape any character.
    Default ``False``.

    .. note::

        Used only for reading files.
    """

    allowUnquotedControlChars: Optional[bool] = None
    """
    If ``True``, allow unquoted control characters (ASCII values 0-31) in strings without escaping them with ``\\``.
    Default ``False``.

    .. note::

        Used only for reading files.
    """

    mode: Optional[Literal["PERMISSIVE", "DROPMALFORMED", "FAILFAST"]] = None
    """
    How to handle parsing errors:
      * ``PERMISSIVE`` - set field value as ``null``, move raw data to :obj:`~columnNameOfCorruptRecord` column.
      * ``DROPMALFORMED`` - skip the malformed row.
      * ``FAILFAST`` - throw an error immediately.

    Default is ``PERMISSIVE``.

    .. note::

        Used only for reading files.
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

    .. note::

        Used only for reading files.
    """

    samplingRatio: Optional[float] = Field(default=None, ge=0, le=1)
    """
    While inferring schema, read the specified fraction of file rows.
    Default ``1``.

    .. note::

        Used only for reading files.
    """

    primitivesAsString: Optional[bool] = None
    """
    If ``True``, infer all primitive types (string, integer, float, boolean) as strings.
    Default ``False``.

    .. note::

        Used only for reading files.
    """

    prefersDecimal: Optional[bool] = None
    """
    If ``True``, infer all floating-point values as ``Decimal``.
    Default ``False``.

    .. note::

        Used only for reading files.
    """

    dropFieldIfAllNull: Optional[bool] = None
    """
    If ``True`` and inferred column is always null or empty array, exclude if from DataFrame schema.
    Default ``False``.

    .. note::

        Used only for reading files.
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

    timezone: Optional[str] = Field(default=None, min_length=1, alias="timeZone")
    """
    Allows to override timezone used for parsing or serializing date and timestamp values.
    By default, ``spark.sql.session.timeZone`` is used.
    """

    locale: Optional[str] = Field(default=None, min_length=1)
    """
    Locale name used to parse dates and timestamps.
    Default is ``en-US``.

    ..  note::

        Used only for reading files.
    """

    class Config:
        known_options = frozenset()
        extra = "allow"

    @slot
    def check_if_supported(self, spark: SparkSession) -> None:
        # always available
        pass

    def __repr__(self):
        options_dict = self.dict(by_alias=True, exclude_none=True, exclude={"multiLine"})
        options_dict = dict(sorted(options_dict.items()))
        options_kwargs = ", ".join(f"{k}={v!r}" for k, v in options_dict.items())
        return f"{self.__class__.__name__}({options_kwargs})"
