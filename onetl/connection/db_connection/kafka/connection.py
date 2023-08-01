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
from onetl.hooks import slot, support_hooks
from onetl.hwm import Statement

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = logging.getLogger(__name__)


@support_hooks
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

        At current version Kafka connection doesn't support batch strategies.

    Examples
    --------

    Connect to Kafka as anonymous user (default):

    .. code:: python

        from onetl.connection import Kafka
        from pyspark.sql import SparkSession

        # Create Spark session with Kafka connector loaded
        maven_packages = Kafka.get_packages(spark_version="3.2.4")
        spark = (
            SparkSession.builder.appName("spark-app-name")
            .config("spark.jars.packages", ",".join(maven_packages))
            .getOrCreate()
        )

        # Create connection
        kafka = Kafka(
            addresses=["mybroker:9092", "anotherbroker:9092"],
            cluster=["my-cluster"],
            spark=spark,
        )

    Connect to Kafka using basic (plain) auth:

    .. code:: python

        # Create Spark session with Kafka connector loaded
        ...

        # Create connection
        kafka = Kafka(
            addresses=["mybroker:9092", "anotherbroker:9092"],
            cluster=["my-cluster"],
            auth=Kafka.SimpleAuth(
                user="me",
                password="password",
            ),
            spark=spark,
        )

    Connect to Kafka using Kerberos auth:

    .. code:: python

        # Create Spark session with Kafka connector loaded
        ...

        # Create connection
        kafka = Kafka(
            addresses=["mybroker:9092", "anotherbroker:9092"],
            cluster=["my-cluster"],
            auth=Kafka.KerberosAuth(
                principal="me@example.com",
                keytab="/path/to/keytab",
            ),
            spark=spark,
        )

    Connect to Kafka with extra options:

    .. code:: python

        # Create Spark session with Kafka connector loaded
        ...

        # Create connection
        kafka = Kafka(
            addresses=["mybroker:9092", "anotherbroker:9092"],
            cluster=["my-cluster"],
            extra={"max.request.size": 1000000},
            spark=spark,
        )

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
    extra: KafkaExtra = KafkaExtra()

    def read_source_as_df(  # type: ignore
        self,
        source: str,
        columns: list[str] | None = None,
        hint: Any | None = None,
        where: Any | None = None,
        df_schema: StructType | None = None,
        start_from: Statement | None = None,
        end_at: Statement | None = None,
        options: KafkaReadOptions = KafkaReadOptions(),  # noqa: B008, WPS404
    ) -> DataFrame:
        result_options: dict = {
            f"kafka.{key}": value for key, value in self.extra.dict(by_alias=True, exclude_none=True).items()
        }
        result_options.update(options.dict(by_alias=True, exclude_none=True))
        result_options.update(self.protocol.get_options(self))

        if self.auth:  # pragma: no cover
            result_options.update(self.auth.get_options(self))

        result_options.update(
            {"kafka.bootstrap.servers": ",".join(self.addresses), "subscribe": source},
        )
        return self.spark.read.format("kafka").options(**result_options).load()

    @slot
    def write_df_to_target(
        self,
        df: DataFrame,
        target: str,
        options: KafkaWriteOptions = KafkaWriteOptions(),  # noqa: B008, WPS404
    ) -> None:
        write_options: dict = {
            f"kafka.{key}": value for key, value in self.extra.dict(by_alias=True, exclude_none=True).items()
        }
        write_options.update(options.dict(by_alias=True, exclude_none=True))
        write_options.update(self.protocol.get_options(self))
        if self.auth:
            write_options.update(self.auth.get_options(self))
        write_options.update({"kafka.bootstrap.servers": ",".join(self.addresses), "topic": target})

        log.info("|%s| Saving data to a topic %r", self.__class__.__name__, target)
        df.write.format("kafka").options(**write_options).save()
        log.info("|%s| Data is successfully written to topic %r", self.__class__.__name__, target)

    @slot
    def get_df_schema(
        self,
        source: str,
        columns: list[str] | None = None,
        options: KafkaReadOptions = KafkaReadOptions(),  # noqa:  WPS404
    ) -> StructType:
        from pyspark.sql.types import (  # noqa:  WPS442
            ArrayType,
            BinaryType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("key", BinaryType(), nullable=True),
                StructField("value", BinaryType(), nullable=True),
                StructField("topic", StringType(), nullable=True),
                StructField("partition", IntegerType(), nullable=True),
                StructField("offset", LongType(), nullable=True),
                StructField("timestamp", TimestampType(), nullable=True),
                StructField("timestampType", IntegerType(), nullable=True),
                StructField(
                    "headers",
                    ArrayType(
                        StructType(
                            [
                                StructField("key", StringType(), nullable=True),
                                StructField("value", BinaryType(), nullable=True),
                            ],
                        ),
                    ),
                ),
            ],
        )

        return schema  # noqa:  WPS331

    @slot
    @classmethod
    def get_packages(
        cls,
        spark_version: str,
        scala_version: str | None = None,
    ) -> list[str]:
        """
        Get package names to be downloaded by Spark. |support_hooks|

        Parameters
        ----------
        spark_version : str
            Spark version in format ``major.minor.patch``.

        scala_version : str, optional
            Scala version in format ``major.minor``.

            If ``None``, ``spark_version`` is used to determine Scala version.

        Examples
        --------

        .. code:: python

            from onetl.connection import Kafka

            Kafka.get_packages(spark_version="3.2.4")
            Kafka.get_packages(spark_version="3.2.4", scala_version="2.13")

        """

        # Connector version is same as Spark, do not perform any additional checks
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
