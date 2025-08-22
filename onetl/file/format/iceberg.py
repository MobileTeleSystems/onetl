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


PROHIBITED_OPTIONS = frozenset(
    (
        # These options should be passed in Spark session config, not file format options
        "spark.*",
    ),
)

log = logging.getLogger(__name__)


@support_hooks
class Iceberg(ReadWriteFileFormat):
    """
    Iceberg file format. |support_hooks|

    Based on `Apache Iceberg <https://iceberg.apache.org/spark-quickstart/>`_ file format.

    Supports reading/writing files with Iceberg table format.

    .. dropdown:: Version compatibility

        * Spark versions: 3.2.x - 3.5.x
        * Java versions: 8 - 20
        * Iceberg versions: 1.4.0 - 1.9.x

        See documentation from link above.

    .. versionadded:: 0.14.0

    Examples
    --------

    .. tabs::

        .. code-tab:: py Reading files

            from pyspark.sql import SparkSession
            from onetl.file.format import Iceberg

            # Create Spark session with Iceberg package loaded
            maven_packages = Iceberg.get_packages(package_version="1.9.2", spark_version="3.5")
            spark = (
                SparkSession.builder.appName("spark-app-name")
                .config("spark.jars.packages", ",".join(maven_packages))
                .getOrCreate()
            )

            iceberg = Iceberg()

        .. code-tab:: py Writing files

            # Create Spark session with Iceberg package loaded
            spark = ...

            from onetl.file.format import Iceberg

            iceberg = Iceberg()
    """

    name: ClassVar[str] = "iceberg"

    class Config:
        known_options = frozenset()
        prohibited_options = PROHIBITED_OPTIONS
        extra = "allow"

    @slot
    @classmethod
    def get_packages(
        cls,
        package_version: str,
        spark_version: str,
        scala_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        See `Maven package index <https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-spark>`_
        for all available packages.

        .. versionadded:: 0.14.0

        Parameters
        ----------
        package_version : str
            Iceberg package version in format ``major.minor.patch``.

        spark_version : str
            Spark version in format ``major.minor``.

        scala_version : str, optional
            Scala version in format ``major.minor``.

            If ``None``, ``spark_version`` is used to determine Scala version.

        Returns
        -------
        list[str]
            List of Maven coordinates.
        """

        version = Version(package_version).min_digits(3)
        spark_ver = Version(spark_version).min_digits(2)
        scala_ver = Version(scala_version).min_digits(2) if scala_version else get_default_scala_version(spark_ver)
        return [
            f"org.apache.iceberg:iceberg-spark-runtime-{spark_ver.format('{0}.{1}')}_{scala_ver.format('{0}.{1}')}:{version}",
        ]

    @slot
    def check_if_supported(self, spark: SparkSession) -> None:
        java_class = "org.apache.iceberg.spark.SparkSessionCatalog"

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

    def __repr__(self):
        options_dict = self.dict(by_alias=True, exclude_none=True)
        options_kwargs = ", ".join(f"{k}={v!r}" for k, v in options_dict.items())
        return f"{self.__class__.__name__}({options_kwargs})"
