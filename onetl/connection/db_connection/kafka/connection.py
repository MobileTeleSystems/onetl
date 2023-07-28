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

import logging
from typing import TYPE_CHECKING, Any, List, Optional

from etl_entities.instance import Cluster
from pydantic import validator

from onetl._util.scala import get_default_scala_version
from onetl._util.version import Version
from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.kafka.dialect import KafkaDialect
from onetl.connection.db_connection.kafka.extra import KafkaExtra
from onetl.connection.db_connection.kafka.kafka_auth import KafkaAuth
from onetl.connection.db_connection.kafka.kafka_basic_auth import KafkaBasicAuth
from onetl.connection.db_connection.kafka.kafka_kerberos_auth import KafkaKerberosAuth
from onetl.connection.db_connection.kafka.kafka_plaintext_protocol import (
    KafkaPlaintextProtocol,
)
from onetl.connection.db_connection.kafka.kafka_protocol import KafkaProtocol
from onetl.connection.db_connection.kafka.options import (
    KafkaReadOptions,
    KafkaWriteOptions,
)
from onetl.hwm import Statement

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = logging.getLogger(__name__)


class Kafka(DBConnection):
    """
    This connector is designed to read and write from Kafka in batch mode.

    Based on `official Kafka 0.10+ Source For Spark
    <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>`_.

    .. note::

        This connector is for batch download from kafka and not streaming.

    .. dropdown:: Version compatibility

        * Apache Kafka versions: 0.10 or higher
        * Spark versions: 2.3.x - 3.4.x

    Parameters
    ----------

    addresses : list[str]
        A list of broker addresses, for example ``[192.168.1.10:9092, 192.168.1.11:9092]``.
        The list cannot be empty.

    cluster : Cluster
        Cluster name. Used for HWM and lineage. A cluster field cannot be empty.

    auth : KafkaAuth, default: ``None``
        An attribute that contains a class that generates a Kafka connection configuration.
        It depends on the type of connection to Kafka.

    protocol : KafkaProtocol, default: ``PlaintextProtocol``
        Class containing connection parameters. If the protocol parameter is not specified, then the parameter will be
        passed ``PLAINTEXT``, otherwise the ``SASL_PLAINTEXT`` parameter will be passed to the
        ``kafka.security.protocol`` option
    extra: dict, default: ``None``
        A dictionary of additional properties to be used when connecting to Kafka. These are typically
        Kafka-specific properties that control behavior of the producer or consumer.

        For example: {"group.id": "myGroup"}

        Be aware of options that populated from connection
        attributes (like "bootstrap.servers") are not allowed to override.

        See Connection `producer options documentation <https://kafka.apache.org/documentation/#producerconfigs>`_,
        `consumer options documentation <https://kafka.apache.org/documentation/#consumerconfigs>`_
        for more details.

    .. warning::

        If there is no file on the driver, then Spark will not be able to build a query execution plan,
        if there is no file on the executors, then they will not be able to read the data.

    .. warning::

        At current version Kafka connection doesn't support batch strategies.

    Examples
    --------

    Connect to Kafka as anonynous user (default):

    .. code:: python

        kafka = Kafka(auth=None)

    Connect to Kafka using basic (plain) auth:

    .. code:: python

        kafka = Kafka(
            auth=Kafka.SimpleAuth(
                user="me",
                password="password",
            ),
        )

    Connect to Kafka using Kerberos auth:

    .. code:: python

        kafka = Kafka(
            auth=Kafka.KerberosAuth(
                principal="me@example.com",
                keytab="/path/to/keytab",
            ),
        )

    Connect to Kafka with extra options:

    .. code:: python

        kafka = Kafka(auth=None, extra={"max.request.size": 1000000})

    """

    BasicAuth = KafkaBasicAuth
    KerberosAuth = KafkaKerberosAuth
    ReadOptions = KafkaReadOptions
    WriteOptions = KafkaWriteOptions
    Extra = KafkaExtra
    Dialect = KafkaDialect
    PlaintextProtocol = KafkaPlaintextProtocol
    addresses: List[str]
    cluster: Cluster
    auth: Optional[KafkaAuth] = None
    protocol: KafkaProtocol = PlaintextProtocol()
    extra: Extra = Extra()  # type: ignore

    def read_source_as_df(  # type: ignore
        self,
        source: str,
        columns: list[str] | None = None,
        hint: Any | None = None,
        where: Any | None = None,
        df_schema: StructType | None = None,
        start_from: Statement | None = None,
        end_at: Statement | None = None,
        **kwargs,
    ) -> DataFrame:
        options: dict = {  # pragma: no cover
            f"kafka.{key}": value
            for key, value in self.extra.dict(by_alias=True, exclude_none=True).items()  # type: ignore[attr-defined]
        }
        options.update(self.protocol.get_options(self))  # pragma: no cover

        if self.auth:  # pragma: no cover
            options.update(self.auth.get_options(self))  # pragma: no cover

        options.update(  # pragma: no cover
            {"kafka.bootstrap.servers": ",".join(self.addresses), "subscribe": source},
        )
        return self.spark.read.format("kafka").options(**options).load()  # pragma: no cover

    def write_df_to_target(self, df: DataFrame, target: str) -> None:
        pass

    @classmethod
    def get_package_spark(
        cls,
        spark_version: str,
        scala_version: str | None = None,
    ) -> list[str]:
        spark_ver = Version.parse(spark_version)
        scala_ver = Version.parse(scala_version) if scala_version else get_default_scala_version(spark_ver)
        return [
            f"org.apache.spark:spark-sql-kafka-0-10_{scala_ver.digits(2)}:{spark_ver.digits(3)}",
        ]

    @property
    def instance_url(self):
        return "kafka://" + self.cluster

    def check(self):
        return self

    @validator("addresses")
    def _validate_addresses(cls, value):
        if not value:
            raise ValueError("Passed empty parameter 'addresses'")
        return value
