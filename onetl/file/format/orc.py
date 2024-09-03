# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
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
    ("mergeSchema",),
)

WRITE_OPTIONS = frozenset(
    (
        "compression",
        "orc.*",
    ),
)


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

    .. versionadded:: 0.9.0

    Examples
    --------

    Describe options how to read from/write to ORC file with specific options:

    .. code:: python

        orc = ORC(compression="snappy")

    """

    name: ClassVar[str] = "orc"

    class Config:
        known_options = READ_OPTIONS | WRITE_OPTIONS
        prohibited_options = PROHIBITED_OPTIONS
        extra = "allow"

    @slot
    def check_if_supported(self, spark: SparkSession) -> None:
        # always available
        pass
