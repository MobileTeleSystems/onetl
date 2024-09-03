# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar

from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.spark import get_spark_version
from onetl._util.version import Version
from onetl.exception import MISSING_JVM_CLASS_MSG
from onetl.file.format.file_format import ReadWriteFileFormat
from onetl.hooks import slot, support_hooks

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

READ_OPTIONS = frozenset(
    (
        "dataAddress",
        "treatEmptyValuesAsNulls",
        "setErrorCellsToFallbackValues",
        "usePlainNumberFormat",
        "inferSchema",
        "addColorColumns",
        "timestampFormat",
        "maxRowsInMemory",
        "maxByteArraySize",
        "tempFileThreshold",
        "excerptSize",
        "workbookPassword",
    ),
)

WRITE_OPTIONS = frozenset(
    (
        "dataAddress",
        "dateFormat",
        "timestampFormat",
    ),
)

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

    .. note ::

        You can pass any option to the constructor, even if it is not mentioned in this documentation.
        **Option names should be in** ``camelCase``!

        The set of supported options depends on Spark version. See link above.

    .. versionadded:: 0.9.4

    Examples
    --------

    Describe options how to read from/write to Excel file with specific options:

    .. code:: python

        from onetl.file.format import Excel
        from pyspark.sql import SparkSession

        # Create Spark session with Excel package loaded
        maven_packages = Excel.get_packages(spark_version="3.5.1")
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        excel = Excel(
            header=True,
            inferSchema=True,
        )

    """

    name: ClassVar[str] = "excel"

    header: bool = False

    class Config:
        known_options = READ_OPTIONS | WRITE_OPTIONS
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
