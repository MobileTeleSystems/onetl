#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from typing_extensions import Literal

from onetl.file.format.file_format import ReadOnlyFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


READ_WRITE_OPTIONS = {
    "dateFormat",
    "enableDateTimeParsingFallback",
    "timestampFormat",
    "timestampNTZFormat",
    "timeZone",
}

READ_OPTIONS = {
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
}

WRITE_OPTIONS = {
    "compression",
    "ignoreNullFields",
}


@support_hooks
class JSON(ReadOnlyFileFormat):
    """
    JSON file format. |support_hooks|

    Based on `Spark JSON Files <https://spark.apache.org/docs/latest/sql-data-sources-json.html>`_ file format.

    Reads files with ``.json`` extension with content like:

    .. code-block:: json
        :caption: example.json

        [
            {"key": "value1"},
            {"key": "value2"}
        ]

    .. warning::

        Does **NOT** support writing. Use :obj:`JSONLine <onetl.file.format.jsonline.JSONLine>` instead.

    .. note ::

        You can pass any option to the constructor, even if it is not mentioned in this documentation.

        The set of supported options depends on Spark version.

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
    @classmethod
    def check_if_supported(cls, spark: SparkSession) -> None:
        # always available
        pass
