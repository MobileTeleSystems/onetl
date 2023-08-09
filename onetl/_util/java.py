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

from typing import TYPE_CHECKING, Any

from onetl._util.version import Version

if TYPE_CHECKING:
    from py4j.java_gateway import JavaGateway, JavaObject
    from pyspark.sql import SparkSession


def get_java_version(spark_session: SparkSession) -> Version:
    """
    Get Java version Spark is running in
    """
    # 1.8.0_292
    # 11.0.4
    java_version: str = spark_session._jvm.System.getProperty("java.version")  # type: ignore
    # 1.8.0.292
    java_version = java_version.replace("_", ".")
    if java_version.startswith("1."):
        # 8.0.292
        java_version = java_version[2:]
    return Version.parse(java_version)


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


def set_java_object_field(obj: JavaObject, field_name: str, value: Any):
    """
    Set Java class field to specific value.

    If field is private, temporary disable private field check using Java Reflection API.

    See https://docs.oracle.com/javase/8/docs/api/java/lang/reflect/Field.html
    """
    klass = obj.getClass()
    field = klass.getDeclaredField(field_name)
    private = not field.isAccessible()
    if private:
        field.setAccessible(True)
    field.set(obj, value)
    if private:
        field.setAccessible(False)
