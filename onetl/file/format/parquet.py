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

PARQUET_LIBRARY_OPTIONS = frozenset(
    ("parquet.*",),
)


@support_hooks
class Parquet(ReadWriteFileFormat):
    """
    Parquet file format (columnar). [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

    Based on [Spark Parquet Files](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) file format.

    Supports reading/writing files with `.parquet` extension.

    !!! success "Added in 0.9.0"

    Examples
    --------

    !!! note

        You can pass any option mentioned in
        [official documentation](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html).
        **Option names should be in** `camelCase`!

        The set of supported options depends on Spark version.

        You may also set options mentioned
        [parquet-hadoop documentation](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/README.md).
        They are prefixed with `parquet.` with dots in names,
        so instead of calling constructor `Parquet(parquet.option=True)` (invalid in Python)
        you should call method `Parquet.parse({"parquet.option": True})`.

    === "Reading files"

        ```python
        from onetl.file.format import Parquet

        parquet = Parquet(mergeSchema=True)
        ```

    === "Writing files"

        ```python
        from onetl.file.format import Parquet

        parquet = Parquet.parse(
            {
                "compression": "snappy",
                # Enable Bloom filter for columns 'id' and 'name'
                "parquet.bloom.filter.enabled#id": True,
                "parquet.bloom.filter.enabled#name": True,
                # Set expected number of distinct values for column 'id'
                "parquet.bloom.filter.expected.ndv#id": 10_000_000,
                # other options
            }
        )
        ```
    """

    name: ClassVar[str] = "parquet"

    mergeSchema: bool | None = None
    """
    Merge schemas of all Parquet files being read into a single schema.
    By default, Spark config option `spark.sql.parquet.mergeSchema` value is used (`false`).

    !!! note

        Used only for reading files.
    """

    compression: str | Literal["uncompressed", "snappy", "gzip", "lzo", "brotli", "lz4", "lz4raw", "zstd"] | None = None
    """
    Compression codec of the Parquet files.
    By default, Spark config option `spark.sql.parquet.compression.codec` value is used (`snappy`).

    !!! note

        Used only for writing files.
    """
    model_config = ConfigDict(
        known_options=PARQUET_LIBRARY_OPTIONS,  # type: ignore[typeddict-unknown-key]
        prohibited_options=PROHIBITED_OPTIONS,  # type: ignore[typeddict-unknown-key]
        extra="allow",
    )

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
