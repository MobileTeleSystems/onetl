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

from onetl.file.format.file_format import ReadWriteFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

PROHIBITED_OPTIONS = frozenset(
    (
        # These options should be passed in Spark session config, not file format options
        "spark.*",
    ),
)

READ_OPTIONS = frozenset(
    (
        "datetimeRebaseMode",
        "int96RebaseMode",
        "mergeSchema",
    ),
)

WRITE_OPTIONS = frozenset(
    (
        "compression",
        "parquet.*",
    ),
)


@support_hooks
class Parquet(ReadWriteFileFormat):
    """
    Parquet file format. |support_hooks|

    Based on `Spark Parquet Files <https://spark.apache.org/docs/latest/sql-data-sources-parquet.html>`_ file format.

    Supports reading/writing files with ``.parquet`` extension.

    .. note ::

        You can pass any option to the constructor, even if it is not mentioned in this documentation.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version. See link above.

    Examples
    --------

    Describe options how to read from/write to Parquet file with specific options:

    .. code:: python

        parquet = Parquet(compression="snappy")

    """

    name: ClassVar[str] = "parquet"

    class Config:
        known_options = READ_OPTIONS | WRITE_OPTIONS
        prohibited_options = PROHIBITED_OPTIONS
        extra = "allow"

    @slot
    def check_if_supported(self, spark: SparkSession) -> None:
        # always available
        pass
