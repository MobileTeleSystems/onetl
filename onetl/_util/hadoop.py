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

from typing import TYPE_CHECKING

from onetl._util.version import Version

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_hadoop_version(spark_session: SparkSession) -> Version:
    """
    Get version of Hadoop libraries embedded to Spark
    """
    jvm = spark_session._jvm  # noqa: WPS437
    version_info = jvm.org.apache.hadoop.util.VersionInfo  # type: ignore[union-attr]
    hadoop_version: str = version_info.getVersion()
    return Version.parse(hadoop_version)


def get_hadoop_config(spark_session: SparkSession):
    """
    Get ``org.apache.hadoop.conf.Configuration`` object
    """
    return spark_session.sparkContext._jsc.hadoopConfiguration()
