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


def get_scala_version(spark_session: SparkSession) -> Version:
    """
    Get Scala version Spark is compiled with
    """
    properties = spark_session._jvm.scala.util.Properties  # type: ignore
    scala_version: str = properties.versionNumberString()
    return Version.parse(scala_version)


def get_default_scala_version(spark_version: Version) -> Version:
    """
    Get default Scala version for specific Spark version
    """
    if spark_version.major < 3:
        return Version(2, 11)
    return Version(2, 12)
