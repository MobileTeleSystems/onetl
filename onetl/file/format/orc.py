# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Optional, Union

from typing_extensions import Literal

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

ORC_JAVA_OPTIONS = frozenset(
    ("orc.*",),
)


@support_hooks
class ORC(ReadWriteFileFormat):
    """
    ORC file format (columnar). |support_hooks|

    Based on `Spark ORC Files <https://spark.apache.org/docs/latest/sql-data-sources-orc.html>`_ file format.

    Supports reading/writing files with ``.orc`` extension.

    .. versionadded:: 0.9.0

    Examples
    --------

    .. note ::

        You can pass any option mentioned in
        `official documentation <https://spark.apache.org/docs/latest/sql-data-sources-orc.html>`_.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version.

        You may also set options mentioned `orc-java documentation <https://orc.apache.org/docs/core-java-config.html>`_.
        They are prefixed with ``orc.`` with dots in names, so instead of calling constructor ``ORC(orc.option=True)`` (invalid in Python)
        you should call method ``ORC.parse({"orc.option": True})``.

    .. tabs::

        .. code-tab:: py Reading files

            from onetl.file.format import ORC

            orc = ORC(mergeSchema=True)

        .. tab:: Writing files

            .. code:: python

                from onetl.file.format import ORC

                orc = ORC.parse(
                    {
                        "compression": "snappy",
                        # Enable Bloom filter for columns 'id' and 'name'
                        "orc.bloom.filter.columns": "id,name",
                        # Set Bloom filter false positive probability
                        "orc.bloom.filter.fpp": 0.01,
                        # Do not use dictionary for 'highly_selective_column'
                        "orc.column.encoding.direct": "highly_selective_column",
                        # other options
                    }
                )

    """

    name: ClassVar[str] = "orc"

    mergeSchema: Optional[bool] = None
    """
    Merge schemas of all ORC files being read into a single schema.
    By default, Spark config option ``spark.sql.orc.mergeSchema`` value is used (``False``).

    .. note::

        Used only for reading files.
    """

    compression: Union[
        str,
        Literal["uncompressed", "snappy", "zlib", "lzo", "zstd", "lz4"],
        None,
    ] = None
    """
    Compression codec of the ORC files.
    By default, Spark config option ``spark.sql.orc.compression.codec`` value is used (``snappy``).

    .. note::

        Used only for writing files.
    """

    class Config:
        known_options = ORC_JAVA_OPTIONS
        prohibited_options = PROHIBITED_OPTIONS
        extra = "allow"

    @slot
    def check_if_supported(self, spark: SparkSession) -> None:
        # always available
        pass

    def __repr__(self):
        options_dict = self.dict(by_alias=True, exclude_none=True)
        options_dict = dict(sorted(options_dict.items()))
        if any("." in field for field in options_dict.keys()):
            return f"{self.__class__.__name__}.parse({options_dict})"

        options_kwargs = ", ".join(f"{k}={v!r}" for k, v in options_dict.items())
        return f"{self.__class__.__name__}({options_kwargs})"
