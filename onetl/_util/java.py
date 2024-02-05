# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
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
