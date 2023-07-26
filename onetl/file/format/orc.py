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

READ_OPTIONS = {
    "mergeSchema",
}

WRITE_OPTIONS = {
    "compression",
    "orc.*",
}


@support_hooks
class ORC(ReadWriteFileFormat):
    """
    ORC file format. |support_hooks|

    Based on `Spark ORC Files <https://spark.apache.org/docs/latest/sql-data-sources-orc.html>`_ file format.

    Supports reading/writing files with ``.orc`` extension.

    .. note ::

        You can pass any option to the constructor, even if it is not mentioned in this documentation.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version. See link above.

    Examples
    --------

    Describe options how to read from/write to ORC file with specific options:

    .. code:: python

        orc = ORC(compression="snappy")

    """

    name: ClassVar[str] = "orc"

    class Config:
        known_options = READ_OPTIONS | WRITE_OPTIONS
        extra = "allow"

    @slot
    @classmethod
    def check_if_supported(cls, spark: SparkSession) -> None:
        # always available
        pass
