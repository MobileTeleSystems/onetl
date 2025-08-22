# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
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
    return spark_session._sc._gateway  # noqa: WPS437 # type: ignore[attr-defined]


def try_import_java_class(spark_session: SparkSession, name: str):
    """
    Try to import Java class by name
    """
    gateway = get_java_gateway(spark_session)
    klass = getattr(gateway.jvm, name)
    gateway.help(klass, display=False)
    return klass


def start_callback_server(spark_session: SparkSession):
    """
    Start Py4J callback server. Important to receive Java events on Python side,
    e.g. in Spark Listener implementations.
    """
    from pyspark.java_gateway import ensure_callback_server_started

    gateway = get_java_gateway(spark_session)
    ensure_callback_server_started(gateway)
