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
from contextlib import closing
from typing import TYPE_CHECKING, Any, List, Optional

from etl_entities.instance import Cluster
from pydantic import root_validator, validator

from onetl._util.java import try_import_java_class
from onetl._util.scala import get_default_scala_version
from onetl._util.spark import get_spark_version
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
    TopicExistBehaviorKafka,
)
from onetl.connection.db_connection.kafka.slots import KafkaSlots
from onetl.exception import MISSING_JVM_CLASS_MSG, TargetAlreadyExistsError
from onetl.hooks import slot, support_hooks
from onetl.hwm import Statement
from onetl.log import log_with_indent

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
        * Spark versions: 2.4.x - 3.4.x

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
    Slots = KafkaSlots
    cluster: Cluster
    addresses: List[str]
    auth: Optional[KafkaAuth] = None
    protocol: KafkaProtocol = PlaintextProtocol()
    extra: KafkaExtra = KafkaExtra()

    @slot
    def check(self):
        log.info("|%s| Checking connection availability...", self.__class__.__name__)
        self._log_parameters()  # type: ignore

        try:
            self._get_topics()
            log.info("|%s| Connection is available.", self.__class__.__name__)
        except Exception as e:
            log.exception("|%s| Connection is unavailable", self.__class__.__name__)
            raise RuntimeError("Connection is unavailable") from e
        return self

    @slot
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
        result_options = {f"kafka.{key}": value for key, value in self._get_connection_properties().items()}
        result_options.update(options.dict(by_alias=True, exclude_none=True))
        result_options["subscribe"] = source
        return self.spark.read.format("kafka").options(**result_options).load()

    @slot
    def write_df_to_target(
        self,
        df: DataFrame,
        target: str,
        options: KafkaWriteOptions = KafkaWriteOptions(),  # noqa: B008, WPS404
    ) -> None:
        # Check that the DataFrame doesn't contain any columns not in the schema
        schema: StructType = self.get_df_schema(target)
        required_columns = [field.name for field in schema.fields if not field.nullable]
        optional_columns = [field.name for field in schema.fields if field.nullable]
        schema_field_names = {field.name for field in schema.fields}
        df_column_names = set(df.columns)
        if not df_column_names.issubset(schema_field_names):
            invalid_columns = df_column_names - schema_field_names
            raise ValueError(
                f"Invalid column names: {invalid_columns}. Expected columns: {required_columns} (required),"
                f" {optional_columns} (optional)",
            )

        # Check that the DataFrame doesn't contain a 'headers' column with includeHeaders=False
        if not getattr(options, "includeHeaders", True) and "headers" in df.columns:
            raise ValueError("Cannot write 'headers' column with kafka.WriteOptions(includeHeaders=False)")

        if "topic" in df.columns:
            log.warning("The 'topic' column in the DataFrame will be overridden with value %r", target)

        write_options = {f"kafka.{key}": value for key, value in self._get_connection_properties().items()}
        write_options.update(options.dict(by_alias=True, exclude_none=True, exclude={"if_exists"}))
        write_options["topic"] = target

        # As of Apache Spark version 3.4.1, the mode 'error' is not functioning as expected.
        # This issue has been reported and can be tracked at:
        # https://issues.apache.org/jira/browse/SPARK-44774
        mode = options.if_exists
        if mode == TopicExistBehaviorKafka.ERROR:
            if target in self._get_topics():
                raise TargetAlreadyExistsError

        log.info("|%s| Saving data to a topic %r", self.__class__.__name__, target)
        df.write.format("kafka").mode(mode).options(**write_options).save()
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
                StructField("value", BinaryType(), nullable=False),
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
        if spark_ver < (2, 4):
            # Kafka connector for Spark 2.3 is build with Kafka client 0.10.0.1 which does not support
            # passing `sasl.jaas.config` option. It is supported only in 0.10.2.0,
            # see https://issues.apache.org/jira/browse/KAFKA-4259
            # Old client requires generating JAAS file and placing it to filesystem, which is not secure.
            raise ValueError(f"Spark version must be at least 2.4, got {spark_ver}")

        scala_ver = Version.parse(scala_version) if scala_version else get_default_scala_version(spark_ver)
        return [
            f"org.apache.spark:spark-sql-kafka-0-10_{scala_ver.digits(2)}:{spark_ver.digits(3)}",
        ]

    @property
    def instance_url(self):
        return "kafka://" + self.cluster

    @root_validator(pre=True)
    def _get_addresses_by_cluster(cls, values):
        cluster = values.get("cluster")
        addresses = values.get("addresses")
        if not addresses:
            cluster_addresses = cls.Slots.get_cluster_addresses(cluster) or []
            if cluster_addresses:
                log.debug("|%s| Set cluster %r addresses: %r", cls.__name__, cluster, cluster_addresses)
                values["addresses"] = cluster_addresses
            else:
                raise ValueError("Passed empty parameter 'addresses'")
        return values

    @validator("cluster")
    def _validate_cluster_name(cls, cluster):
        log.debug("|%s| Normalizing cluster %r name ...", cls.__name__, cluster)
        validated_cluster = cls.Slots.normalize_cluster_name(cluster) or cluster
        if validated_cluster != cluster:
            log.debug("|%s|   Got %r", cls.__name__, validated_cluster)

        log.debug("|%s| Checking if cluster %r is a known cluster ...", cls.__name__, validated_cluster)
        known_clusters = cls.Slots.get_known_clusters()
        if known_clusters and validated_cluster not in known_clusters:
            raise ValueError(
                f"Cluster {validated_cluster!r} is not in the known clusters list: {sorted(known_clusters)!r}",
            )

        return validated_cluster

    @validator("addresses")
    def _validate_addresses(cls, value, values):
        cluster = values.get("cluster")

        log.debug("|%s| Normalizing addresses %r names ...", cls.__name__, value)

        validated_addresses = [cls.Slots.normalize_address(address, cluster) or address for address in value]
        if validated_addresses != value:
            log.debug("|%s| Got %r", cls.__name__, validated_addresses)

        cluster_addresses = set(cls.Slots.get_cluster_addresses(cluster) or [])
        unknown_addresses = {address for address in validated_addresses if address not in cluster_addresses}
        if cluster_addresses and unknown_addresses:
            raise ValueError(f"Cluster {cluster!r} does not contain addresses {unknown_addresses!r}")

        return validated_addresses

    @validator("spark")
    def _check_spark_version(cls, spark):
        spark_version = get_spark_version(spark)
        if spark_version < (2, 4):
            raise ValueError(f"Spark version must be at least 2.4, got {spark_version}")

        return spark

    @validator("spark")
    def _check_java_class_imported(cls, spark):
        java_class = "org.apache.spark.sql.kafka010.KafkaSourceProvider"

        try:
            try_import_java_class(spark, java_class)
        except Exception as e:
            spark_version = get_spark_version(spark).digits(2)
            msg = MISSING_JVM_CLASS_MSG.format(
                java_class=java_class,
                package_source=cls.__name__,
                args=f"spark_version='{spark_version}'",
            )
            if log.isEnabledFor(logging.DEBUG):
                log.debug("Missing Java class", exc_info=e, stack_info=True)
            raise ValueError(msg) from e
        return spark

    def _get_connection_properties(self) -> dict:
        result = {"bootstrap.servers": ",".join(self.addresses)}
        result.update(self.extra.dict(by_alias=True, exclude_none=True))
        result.update(self.protocol.get_options(self))

        if self.auth:
            result.update(self.auth.get_options(self))
        return result

    def _get_java_consumer(self):
        connection_properties = self._get_connection_properties()
        connection_properties.update(
            {
                # Consumer cannot be created without these options
                # They are set by Spark internally, but for manual consumer creation we need to pass them explicitly
                "key.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                "value.deserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
            },
        )
        jvm = self.spark._jvm
        consumer_class = jvm.org.apache.kafka.clients.consumer.KafkaConsumer
        return consumer_class(connection_properties)

    def _get_topics(self, timeout: int = 1) -> set[str]:
        jvm = self.spark._jvm
        duration = jvm.java.time.Duration.ofSeconds(timeout)  # type: ignore[union-attr]
        consumer = self._get_java_consumer()
        with closing(consumer):
            topics = consumer.listTopics(duration)
            return set(topics)

    def _log_parameters(self):
        log.info("|Spark| Using connection parameters:")
        log_with_indent("type = %s", self.__class__.__name__)
        log_with_indent("addresses = %r", self.addresses)
        log_with_indent("cluster = %r", self.cluster)
        log_with_indent("protocol = %r", self.protocol)
        log_with_indent("auth = %r", self.auth)
        log_with_indent("extra = %r", self.extra.dict(by_alias=True, exclude_none=True))
