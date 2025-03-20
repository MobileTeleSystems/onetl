# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Optional

try:
    from pydantic.v1 import ByteSize, SecretStr
except (ImportError, AttributeError):
    from pydantic import ByteSize, SecretStr  # type: ignore[no-redef, assignment]

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
    Excel file format. |support_hooks|

    Based on `Spark Excel <https://github.com/crealytics/spark-excel>`_ file format.

    Supports reading/writing files with ``.xlsx`` (read/write) and ``.xls`` (read only) extensions.

    .. dropdown:: Version compatibility

        * Spark versions: 3.2.x - 3.5.x

            .. warning::

                Not all combinations of Spark version and package version are supported.
                See `Maven index <https://mvnrepository.com/artifact/com.crealytics/spark-excel>`_
                and `official documentation <https://github.com/crealytics/spark-excel>`_.

        * Java versions: 8 - 20

        See documentation from link above.

    .. versionadded:: 0.9.4

    Examples
    --------

    .. note ::

        You can pass any option mentioned in
        `official documentation <https://github.com/crealytics/spark-excel>`_.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on ``spark-excel`` package version.

    .. tabs::

        .. code-tab:: py Reading files

            from pyspark.sql import SparkSession
            from onetl.file.format import Excel

            # Create Spark session with Excel package loaded
            maven_packages = Excel.get_packages(spark_version="3.5.1")
            spark = (
                SparkSession.builder.appName("spark-app-name")
                .config("spark.jars.packages", ",".join(maven_packages))
                .getOrCreate()
            )

            excel = Excel(header=True, inferSchema=True)

        .. code-tab:: py Writing files

            # Create Spark session with Excel package loaded
            spark = ...

            from onetl.file.format import XML

            excel = Excel(header=True, dataAddress="'Sheet1'!A1")
    """

    name: ClassVar[str] = "excel"

    header: bool = False
    """
    If ``True``, the first row in file is conditioned as a header.
    Default ``False``.
    """

    dataAddress: Optional[str] = None
    """
    Cell address used as starting point.
    For example: ``'A1'`` or ``'Sheet1'!A1``
    """

    timestampFormat: Optional[str] = None
    """
    Format string used for parsing or serializing timestamp values.
    Default ``yyyy-mm-dd hh:mm:ss[.fffffffff]``.
    """

    dateFormat: Optional[str] = None
    """
    Format string used for parsing or serializing date values.
    Default ``yyyy-MM-dd``.
    """

    treatEmptyValuesAsNulls: Optional[bool] = None
    """
    If ``True``, empty cells are parsed as ``null`` values.
    If ``False``, empty cells are parsed as empty strings.
    Default ``True``.

    .. note::

        Used only for reading files.
    """

    setErrorCellsToFallbackValues: Optional[bool] = None
    """
    If ``True``, cells containing ``#N/A`` value are replaced with default value for column type,
    e.g. 0 for ``IntegerType()``. If ``False``, ``#N/A`` values are replaced with ``null``.
    Default ``False``.

    .. note::

        Used only for reading files.
    """

    usePlainNumberFormat: Optional[bool] = None
    """
    If ``True``, read or write numeric values with plain format, without using scientific notation or rounding.
    Default ``False``.
    """

    inferSchema: Optional[bool] = None
    """
    If ``True``, infer DataFrame schema based on cell content.
    If ``False`` and no explicit DataFrame schema is passed, all columns are ``StringType()``.

    .. note::

        Used only for reading files.
    """

    workbookPassword: Optional[SecretStr] = None
    """
    If Excel file is encrypted, provide password to open it.

    .. note::

        Used only for reading files. Cannot be used to write files.
    """

    maxRowsInMemory: Optional[int] = None
    """
    If set, use streaming reader and fetch only specified number of rows per iteration.
    This reduces memory usage for large files.
    Default ``None``, which means reading the entire file content to memory.

    .. warning::

        Can be used only with ``.xlsx`` files, but fails on ``.xls``.

    .. note::

        Used only for reading files.
    """

    maxByteArraySize: Optional[ByteSize] = None
    """
    If set, overrides memory limit (in bytes) of byte array size used for reading rows from input file.
    Default ``0``, which means using default limit.

    See `IOUtils.setByteArrayMaxOverride <https://poi.apache.org/apidocs/5.0/org/apache/poi/util/IOUtils.html#setByteArrayMaxOverride-int->`_
    documentation.

    .. note::

        Used only for reading files.
    """

    tempFileThreshold: Optional[ByteSize] = None
    """
    If value is greater than 0, large zip entries will be written to temporary files after reaching this threshold.
    If value is 0, all zip entries will be written to temporary files.
    If value is -1, no temp files will be created, which may cause errors if zip entry is larger than 2GiB.

    .. note::

        Used only for reading files.
    """

    excerptSize: Optional[int] = None
    """
    If ``inferSchema=True``, set number of rows to infer schema from.
    Default ``10``.

    .. note::

        Used only for reading files.
    """

    class Config:
        known_options = frozenset()
        extra = "allow"

    @slot
    @classmethod
    def get_packages(
        cls,
        spark_version: str,
        scala_version: str | None = None,
        package_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        .. warning::

            Not all combinations of Spark version and package version are supported.
            See `Maven index <https://mvnrepository.com/artifact/com.crealytics/spark-excel>`_
            and `official documentation <https://github.com/crealytics/spark-excel>`_.

        .. versionadded:: 0.9.4

        Parameters
        ----------
        spark_version : str
            Spark version in format ``major.minor.patch``.

        scala_version : str, optional
            Scala version in format ``major.minor``.

            If ``None``, ``spark_version`` is used to determine Scala version.

        package_version : str, optional
            Package version in format ``major.minor.patch``. Default is ``0.20.4``.

            .. warning::

                Version ``0.14`` and below are not supported.

            .. note::

                It is not guaranteed that custom package versions are supported.
                Tests are performed only for default version.

        Examples
        --------

        .. code:: python

            from onetl.file.format import Excel

            Excel.get_packages(spark_version="3.5.1")
            Excel.get_packages(spark_version="3.5.1", scala_version="2.12")
            Excel.get_packages(
                spark_version="3.5.1",
                scala_version="2.12",
                package_version="0.20.4",
            )

        """

        if package_version:
            version = Version(package_version)
            if version < Version("0.15"):
                # format="com.crealytics.spark.excel" does not support reading folder with files
                # format="excel" was added only in 0.14, but Maven package for 0.14 has different naming convention than recent versions.
                # So using 0.15 as the lowest supported version.
                raise ValueError(f"Package version should be at least 0.15, got {package_version}")
            log.warning("Passed custom package version %r, it is not guaranteed to be supported", package_version)
        else:
            version = Version("0.20.4")

        spark_ver = Version(spark_version).min_digits(3)
        if spark_ver < Version("3.2"):
            # Actually, Spark 2.4 is supported, but packages are built only for Scala 2.12
            # when default pyspark==2.4.1 is built with Scala 2.11.
            # See https://github.com/crealytics/spark-excel/issues/426
            raise ValueError(f"Spark version should be at least 3.2, got {spark_version}")

        scala_ver = Version(scala_version).min_digits(2) if scala_version else get_default_scala_version(spark_ver)
        if scala_ver < Version("2.12"):
            raise ValueError(f"Scala version should be at least 2.12, got {scala_ver.format('{0}.{1}')}")

        return [
            f"com.crealytics:spark-excel_{scala_ver.format('{0}.{1}')}:{spark_ver.format('{0}.{1}.{2}')}_{version}",
        ]

    @slot
    def check_if_supported(self, spark: SparkSession) -> None:
        java_class = "com.crealytics.spark.excel.v2.ExcelDataSource"

        try:
            try_import_java_class(spark, java_class)
        except Exception as e:
            spark_version = get_spark_version(spark)
            msg = MISSING_JVM_CLASS_MSG.format(
                java_class=java_class,
                package_source=self.__class__.__name__,
                args=f"spark_version='{spark_version}'",
            )
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Missing Java class", exc_info=e, stack_info=True)
            raise ValueError(msg) from e

    @slot
    def apply_to_reader(self, reader: DataFrameReader) -> DataFrameReader:
        options = self.dict(by_alias=True, exclude_none=True)
        if self.workbookPassword:
            options["workbookPassword"] = self.workbookPassword.get_secret_value()
        return reader.format(self.name).options(**options)

    def __repr__(self):
        options_dict = self.dict(by_alias=True, exclude_none=True)
        options_dict = dict(sorted(options_dict.items()))
        options_kwargs = ", ".join(f"{k}={v!r}" for k, v in options_dict.items())
        return f"{self.__class__.__name__}({options_kwargs})"
