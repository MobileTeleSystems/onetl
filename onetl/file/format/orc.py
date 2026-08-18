# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING, ClassVar, Literal

from pydantic import ConfigDict

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
    ORC file format (columnar). [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

    Based on [Spark ORC Files](https://spark.apache.org/docs/latest/sql-data-sources-orc.html) file format.

    Supports reading/writing files with `.orc` extension.

    !!! success "Added in 0.9.0"

    Examples
    --------

    !!! note

        You can pass any option mentioned in
        [official documentation](https://spark.apache.org/docs/latest/sql-data-sources-orc.html).
        **Option names should be in** `camelCase`!

        The set of supported options depends on Spark version.

        You may also set options mentioned
        [orc-java documentation](https://orc.apache.org/docs/core-java-config.html).
        They are prefixed with `orc.` with dots in names,
        so instead of calling constructor `ORC(orc.option=True)` (invalid in Python)
        you should call method `ORC.parse({"orc.option": True})`.

    === "Reading files"

        ```python
        from onetl.file.format import ORC

        orc = ORC(mergeSchema=True)
        ```

    === "Writing files"

        ```python
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
        ```
    """

    name: ClassVar[str] = "orc"

    mergeSchema: bool | None = None
    """
    Merge schemas of all ORC files being read into a single schema.
    By default, Spark config option `spark.sql.orc.mergeSchema` value is used (`False`).

    !!! note

        Used only for reading files.
    """

    compression: str | Literal["uncompressed", "snappy", "zlib", "lzo", "zstd", "lz4"] | None = None
    """
    Compression codec of the ORC files.
    By default, Spark config option `spark.sql.orc.compression.codec` value is used (`snappy`).

    !!! note

        Used only for writing files.
    """
    model_config = ConfigDict(known_options=ORC_JAVA_OPTIONS, prohibited_options=PROHIBITED_OPTIONS, extra="allow")  # type: ignore[typeddict-unknown-key]

    @slot
    def check_if_supported(self, spark: "SparkSession") -> None:
        # always available
        pass

    def __repr__(self):
        options_dict = self.model_dump(by_alias=True, exclude_none=True)
        options_dict = dict(sorted(options_dict.items()))
        if any("." in field for field in options_dict):
            return f"{self.__class__.__name__}.parse({options_dict})"

        options_kwargs = ", ".join(f"{k}={v!r}" for k, v in options_dict.items())
        return f"{self.__class__.__name__}({options_kwargs})"
