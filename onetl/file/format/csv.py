# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from pydantic import Field

from onetl.file.format.file_format import ReadWriteFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


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
