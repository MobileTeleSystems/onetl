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

from onetl.connection.db_connection.db_connection import DBConnection
from onetl.connection.db_connection.kafka.dialect import KafkaDialect
from onetl.connection.db_connection.kafka.ikafka_auth import IKafkaAuth
from onetl.connection.db_connection.kafka.ikafka_protocol import IKafkaProtocol
from onetl.connection.db_connection.kafka.kafka_basic_auth import KafkaBasicAuth
from onetl.connection.db_connection.kafka.kafka_kerberos_auth import KafkaKerberosAuth
from onetl.connection.db_connection.kafka.kafka_plaintext_protocol import (
    KafkaPlaintextProtocol,
)
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

    auth : IKafkaAuth, default: ``None``
        An attribute that contains a class that generates a Kafka connection configuration.
        It depends on the type of connection to Kafka.

    protocol : IKafkaProtocol, default: ``PlaintextProtocol``
        Class containing connection parameters. If the protocol parameter is not specified, then the parameter will be
        passed ``PLAINTEXT``, otherwise the ``SASL_PLAINTEXT`` parameter will be passed to the
        ``kafka.security.protocol`` option

    .. warning::

        If there is no file on the driver, then Spark will not be able to build a query execution plan,
        if there is no file on the executors, then they will not be able to read the data.

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

    """

    BasicAuth = KafkaBasicAuth
    KerberosAuth = KafkaKerberosAuth
    ReadOptions = KafkaReadOptions
    WriteOptions = KafkaWriteOptions
    Dialect = KafkaDialect
    addresses: List[str]
    cluster: Cluster
    auth: Optional[IKafkaAuth] = None
    PlaintextProtocol = KafkaPlaintextProtocol
    protocol: IKafkaProtocol = PlaintextProtocol()

    def read_source_as_df(  # type: ignore
        self,
        source: str,
        columns: list[str] | None = None,
        hint: Any | None = None,
        where: Any | None = None,
        df_schema: StructType | None = None,
        start_from: Statement | None = None,
        end_at: Statement | None = None,
    ) -> DataFrame:
        pass

    def write_df_to_target(self, df: DataFrame, target: str) -> None:
        pass

    @classmethod
    def get_package_spark(
        cls,
        spark_version: str,
        scala_version: str | None = None,
    ) -> list[str]:
        if not scala_version:
            scala_version = "2.11" if spark_version.startswith("2") else "2.12"
        return [f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"]

    @property
    def instance_url(self):
        return "kafka://" + self.cluster

    def check(self):
        return self

    def get_min_max_bounds(  # type: ignore
        self,
        table: str,
        column: str,
        expression: str | None = None,
        hint: Any | None = None,
        where: Any | None = None,
    ) -> tuple[Any, Any]:
        ...

    @validator("addresses")
    def _validate_addresses(cls, value):  # noqa: N805
        if not value:
            raise ValueError("Passed empty parameter 'addresses'")
        return value
