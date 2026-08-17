# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import logging
from typing import TYPE_CHECKING, ClassVar

from pydantic import ByteSize, ConfigDict, SecretStr

from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.spark import get_spark_version
from onetl._util.version import Version
from onetl.exception import MISSING_JVM_CLASS_MSG
from onetl.file.format.file_format import ReadWriteFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import DataFrameReader, SparkSession

log = logging.getLogger(__name__)


@support_hooks
class Excel(ReadWriteFileFormat):
    """
    Excel file format. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

    Based on [Spark Excel](https://github.com/crealytics/spark-excel) file format.

    Supports reading/writing files with `.xlsx` (read/write) and `.xls` (read only) extensions.

    ??? note "Version compatibility"

        * Spark versions: 3.2.x - 4.0.x

            !!! warning

                Not all combinations of Spark version and package version are supported.
                See [Maven index](https://mvnrepository.com/artifact/dev.mauch/spark-excel)
                and [official documentation](https://github.com/crealytics/spark-excel).

        * Java versions: 8 -- 22

        See documentation from link above.

    !!! success "Added in 0.9.4"

    Examples
    --------

    !!! note

        You can pass any option mentioned in
        [official documentation](https://github.com/crealytics/spark-excel).
        **Option names should be in** `camelCase`!

        The set of supported options depends on `spark-excel` package version.

    === "Reading files"

        ```python
        from pyspark.sql import SparkSession
        from onetl.file.format import Excel

        # Create Spark session with Excel package loaded
        maven_packages = Excel.get_packages(
            package_version="0.31.2",
            spark_version="3.5.8",
        )
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        excel = Excel(header=True, inferSchema=True)
        ```

    === "Writing files"

        ```python
        # Create Spark session with Excel package loaded
        spark = ...

        from onetl.file.format import XML

        excel = Excel(header=True, dataAddress="'Sheet1'!A1")
        ```
    """

    name: ClassVar[str] = "excel"

    header: bool = False
    """
    If `True`, the first row in file is conditioned as a header.
    Default `False`.
    """

    dataAddress: str | None = None
    """
    Cell address used as starting point.
    For example: `'A1'` or `'Sheet1'!A1`
    """

    timestampFormat: str | None = None
    """
    Format string used for parsing or serializing timestamp values.
    Default `yyyy-mm-dd hh:mm:ss[.fffffffff]`.
    """

    dateFormat: str | None = None
    """
    Format string used for parsing or serializing date values.
    Default `yyyy-MM-dd`.
    """

    treatEmptyValuesAsNulls: bool | None = None
    """
    If `True`, empty cells are parsed as `null` values.
    If `False`, empty cells are parsed as empty strings.
    Default `True`.

    !!! note

        Used only for reading files.
    """

    setErrorCellsToFallbackValues: bool | None = None
    """
    If `True`, cells containing `#N/A` value are replaced with default value for column type,
    e.g. 0 for `IntegerType()`. If `False`, `#N/A` values are replaced with `null`.
    Default `False`.

    !!! note

        Used only for reading files.
    """

    usePlainNumberFormat: bool | None = None
    """
    If `True`, read or write numeric values with plain format, without using scientific notation or rounding.
    Default `False`.
    """

    inferSchema: bool | None = None
    """
    If `True`, infer DataFrame schema based on cell content.
    If `False` and no explicit DataFrame schema is passed, all columns are `StringType()`.

    !!! note

        Used only for reading files.
    """

    workbookPassword: SecretStr | None = None
    """
    If Excel file is encrypted, provide password to open it.

    !!! note

        Used only for reading files. Cannot be used to write files.
    """

    maxRowsInMemory: int | None = None
    """
    If set, use streaming reader and fetch only specified number of rows per iteration.
    This reduces memory usage for large files.
    Default `None`, which means reading the entire file content to memory.

    !!! warning

        Can be used only with `.xlsx` files, but fails on `.xls`.

    !!! note

        Used only for reading files.
    """

    maxByteArraySize: ByteSize | None = None
    """
    If set, overrides memory limit (in bytes) of byte array size used for reading rows from input file.
    Default `0`, which means using default limit.

    See [IOUtils.setByteArrayMaxOverride](https://poi.apache.org/apidocs/5.0/org/apache/poi/util/IOUtils.html#setByteArrayMaxOverride-int-)
    documentation.

    !!! note

        Used only for reading files.
    """

    tempFileThreshold: ByteSize | None = None
    """
    If value is greater than 0, large zip entries will be written to temporary files after reaching this threshold.
    If value is 0, all zip entries will be written to temporary files.
    If value is -1, no temp files will be created, which may cause errors if zip entry is larger than 2GiB.

    !!! note

        Used only for reading files.
    """

    excerptSize: int | None = None
    """
    If `inferSchema=True`, set number of rows to infer schema from.
    Default `10`.

    !!! note

        Used only for reading files.
    """
    model_config = ConfigDict(extra="allow", known_options=[])  # type: ignore[typeddict-unknown-key]

    @slot
    @classmethod
    def get_packages(
        cls,
        package_version: str,
        spark_version: str,
        scala_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        !!! warning

            Not all combinations of Spark version and package version are supported.
            See [Maven index](https://mvnrepository.com/artifact/dev.mauch/spark-excel)
            and [official documentation](https://github.com/crealytics/spark-excel).

        !!! success "Added in 0.9.4"
        !!! info "Changed in 0.14.0"
            Maven package `com.crealytics:spark-excel` was renamed to `dev.mauch:spark-excel`.

        Parameters
        ----------
        package_version
            Package version in format `major.minor.patch`.

            !!! info "Changed in 0.14.0"
                This parameter is now mandatory.

        spark_version
            Spark version in format `major.minor.patch`.

        scala_version
            Scala version in format `major.minor`.

            If `None`, `spark_version` is used to determine Scala version.

        Examples
        --------

        ```python
        from onetl.file.format import Excel

        Excel.get_packages(
            package_version="0.31.2",
            spark_version="3.5.8",
        )
        Excel.get_packages(
            package_version="0.31.2",
            spark_version="3.5.8",
            scala_version="2.12",
        )
        ```
        """

        version = Version(package_version)
        if version < Version("0.30"):
            msg = f"Package version should be at least 0.30, got {package_version}"
            raise ValueError(msg)

        spark_ver = Version(spark_version).min_digits(3)
        scala_ver = Version(scala_version).min_digits(2) if scala_version else get_default_scala_version(spark_ver)
        return [
            f"dev.mauch:spark-excel_{scala_ver.format('{0}.{1}')}:{spark_ver.format('{0}.{1}.{2}')}_{version}",
        ]

    @slot
    def check_if_supported(self, spark: "SparkSession") -> None:
        java_class = "dev.mauch.spark.excel.v2.ExcelDataSource"

        try:
            try_import_java_class(spark, java_class)
        except Exception as e:
            spark_version = get_spark_version(spark).format("{0}.{1}.{2}")
            msg = MISSING_JVM_CLASS_MSG.format(
                java_class=java_class,
                package_source=self.__class__.__name__,
                args=f"spark_version='{spark_version}'",
            )
            raise ValueError(msg) from e

    @slot
    def apply_to_reader(self, reader: "DataFrameReader") -> "DataFrameReader":
        options = self.model_dump(by_alias=True, exclude_none=True)
        if self.workbookPassword:
            options["workbookPassword"] = self.workbookPassword.get_secret_value()
        return reader.format(self.name).options(**options)

    def __repr__(self):
        options_dict = self.model_dump(by_alias=True, exclude_none=True)
        options_dict = dict(sorted(options_dict.items()))
        options_kwargs = ", ".join(f"{k}={v!r}" for k, v in options_dict.items())
        return f"{self.__class__.__name__}({options_kwargs})"
