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

if TYPE_CHECKING:
    from py4j.java_gateway import JavaGateway
    from pyspark.sql import SparkSession


def get_java_gateway(spark_session: SparkSession) -> JavaGateway:
    """
    Get py4j Java gateway object
    """
    return spark_session._sc._gateway  # noqa: WPS437  # type: ignore


def try_import_java_class(spark_session: SparkSession, name: str):
    """
    Try to import Java class by name
    """
    gateway = get_java_gateway(spark_session)
    klass = getattr(gateway.jvm, name)
    gateway.help(klass, display=False)
    return klass
