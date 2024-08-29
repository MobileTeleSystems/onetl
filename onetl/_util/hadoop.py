# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING

from onetl._util.version import Version

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_hadoop_version(spark_session: SparkSession) -> Version:
    """
    Get version of Hadoop libraries embedded to Spark
    """
    jvm = spark_session._jvm  # noqa: WPS437 # type: ignore[attr-defined]
    version_info = jvm.org.apache.hadoop.util.VersionInfo  # type: ignore[union-attr]
    hadoop_version: str = version_info.getVersion()
    return Version(hadoop_version)


def get_hadoop_config(spark_session: SparkSession):
    """
    Get ``org.apache.hadoop.conf.Configuration`` object
    """
    return spark_session.sparkContext._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
