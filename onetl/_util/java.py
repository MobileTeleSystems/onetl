# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING

from onetl._util.spark import get_spark_version
from onetl._util.version import Version

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
    gateway = get_java_gateway(spark_session)
    if get_spark_version(spark_session) >= Version("2.4"):
        from pyspark.java_gateway import ensure_callback_server_started

        ensure_callback_server_started(gateway)
        return

    # PySpark 2.3
    if "_callback_server" not in gateway.__dict__ or gateway._callback_server is None:
        from py4j.java_gateway import JavaObject

        gateway.callback_server_parameters.eager_load = True
        gateway.callback_server_parameters.daemonize = True
        gateway.callback_server_parameters.daemonize_connections = True
        gateway.callback_server_parameters.port = 0
        gateway.start_callback_server(gateway.callback_server_parameters)
        cbport = gateway._callback_server.server_socket.getsockname()[1]
        gateway._callback_server.port = cbport
        # gateway with real port
        gateway._python_proxy_port = gateway._callback_server.port
        # get the GatewayServer object in JVM by ID
        java_gateway = JavaObject("GATEWAY_SERVER", gateway._gateway_client)
        # update the port of CallbackClient with real port
        java_gateway.resetCallbackClient(java_gateway.getCallbackClient().getAddress(), gateway._python_proxy_port)
