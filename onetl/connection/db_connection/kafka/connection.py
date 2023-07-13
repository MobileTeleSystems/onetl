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

import hashlib
import logging
import os
import shutil
import textwrap
from typing import TYPE_CHECKING, Any, List, Optional

from etl_entities.instance import Cluster
from pydantic import SecretStr, root_validator, validator

from onetl.connection.db_connection.db_connection import DBConnection
from onetl.hwm import Statement
from onetl.impl import GenericOptions, LocalPath, path_repr

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

log = logging.getLogger(__name__)


PROHIBITED_OPTIONS = frozenset(
    (
        "assign",
        "subscribe",
        "subscribePattern",
        "startingOffsets",
        "startingOffsetsByTimestamp",
        "startingTimestamp",
        "endingOffsets",
        "endingOffsetsByTimestamp",
        "endingOffsets",
        "startingOffsetsByTimestampStrategy",
        "kafka.*",
        "topic",
    ),
)

KNOWN_READ_OPTIONS = frozenset(
    (
        "assign",
        "subscribe",
        "subscribePattern",
        "kafka.bootstrap.servers",
        "startingTimestamp",
        "startingOffsetsByTimestamp",
        "startingOffsets",
        "endingTimestamp",
        "endingOffsetsByTimestamp",
        "endingOffsets",
        "failOnDataLoss",
        "kafkaConsumer.pollTimeoutMs",
        "fetchOffset.numRetries",
        "fetchOffset.retryIntervalMs",
        "maxOffsetsPerTrigger",
        "minOffsetsPerTrigger",
        "maxTriggerDelay",
        "minPartitions",
        "groupIdPrefix",
        "kafka.group.id",
        "includeHeaders",
        "startingOffsetsByTimestampStrategy",
    ),
)

KNOWN_WRITE_OPTIONS = frozenset(
    ("includeHeaders",),
)


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

    user : str, default  ``None``
        User, which have proper access to the Apache Kafka. For example: ``some_user``.

    password : SecretStr, default  ``None``
        Password for Kafka connection.

    .. warning::

        When creating a connector, when specifying `user` parameter, either `password` or `keytab` can be specified. Or
        these parameters for anonymous connection are not specified at all.

    keytab : LocalPath, default  ``None``
        A path to the keytab file. A keytab is a file containing pairs of Kerberos principals and encrypted keys that
        are derived from the Kerberos password. You can use this file to log on to Kerberos without being prompted for
        a password.

    deploy_keytab : bool, default ``True``
        If ``True``, connector deploys a keytab file to all executors.
        It is stored in the current directory of both driver and executor.
        Otherwise, keytab file path should exist on driver and all the executors.
        The configs must be located in the same path.

    .. warning::

        If there is no file on the driver, then Spark will not be able to build a query execution plan,
        if there is no file on the executors, then they will not be able to read the data.

    .. warning::

        When creating a connector, when specifying `user` parameter, either `password` or `keytab` can be specified. Or
        these parameters for anonymous connection are not specified at all.

    Examples
    --------

    .. code:: python

        kafka = Kafka(
            addresses=["mybroker:9020", "anotherbroker:9020"],
            cluster="mycluster",
            user="me",
            keytab="/etc/security/keytabs/me.keytab",
            spark=spark,
        )

    .. code:: python

        kafka = Kafka(
            addresses=["mybroker:9020", "anotherbroker:9020"],
            cluster="mycluster",
            user="me",
            password="pwd",
            spark=spark,
        )

    """

    class ReadOptions(GenericOptions):
        """Reading options for Kafka connector.

        .. note ::

            You can pass any value
            `supported by connector <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>`_,
            even if it is not mentioned in this documentation.

            The set of supported options depends on connector version.

        .. warning::

            Options ``kafka.*``, ``assign``, ``subscribe``, ``subscribePattern``, ``startingOffsets``,
            ``startingOffsetsByTimestamp``, ``startingTimestamp``, ``endingOffsets``, ``endingOffsetsByTimestamp``,
            ``endingOffsets``, ``startingOffsetsByTimestampStrategy``, ``topic`` are populated from connection
            attributes, and cannot be set in ``KafkaReadOptions`` class.

        Examples
        --------

        Read options initialization

        .. code:: python

            Kafka.ReadOptions(
                maxOffsetsPerTrigger=10000,
            )
        """

        class Config:
            prohibited_options = PROHIBITED_OPTIONS
            known_options = KNOWN_READ_OPTIONS
            extra = "allow"

    class WriteOptions(GenericOptions):
        """Writing options for Kafka connector.

        .. note ::

            You can pass any value
            `supported by connector <https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html>`_,
            even if it is not mentioned in this documentation.

            The set of supported options depends on connector version.

        .. warning::

            Options ``kafka.*``, ``assign``, ``subscribe``, ``subscribePattern``, ``startingOffsets``,
            ``startingOffsetsByTimestamp``, ``startingTimestamp``, ``endingOffsets``, ``endingOffsetsByTimestamp``,
            ``endingOffsets``, ``startingOffsetsByTimestampStrategy``, ``topic`` are populated from connection
            attributes, and cannot be set in ``KafkaReadOptions`` class.

        Examples
        --------

        Write options initialization

        .. code:: python

            options = Kafka.WriteOptions(
                includeHeaders=False,
            )
        """

        class Config:
            prohibited_options = PROHIBITED_OPTIONS
            known_options = KNOWN_WRITE_OPTIONS
            extra = "allow"

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

    addresses: List[str]
    cluster: Cluster
    user: Optional[str] = None
    password: Optional[SecretStr] = None
    keytab: Optional[LocalPath] = None
    deploy_keytab: bool = True

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
        ...

    def read_table(  # type: ignore
        self,
        table: str,
        columns: list[str] | None = None,
        hint: Any | None = None,
        where: Any | None = None,
        df_schema: StructType | None = None,
        start_from: Statement | None = None,
        end_at: Statement | None = None,
    ) -> DataFrame:
        ...

    def save_df(self, df: DataFrame, table: str) -> None:
        ...

    def get_min_max_bounds(  # type: ignore
        self,
        table: str,
        column: str,
        expression: str | None = None,
        hint: Any | None = None,
        where: Any | None = None,
    ) -> tuple[Any, Any]:
        ...

    @validator("keytab")
    def _validate_keytab(cls, value):  # noqa: N805
        if not os.path.exists(value):
            raise ValueError(
                f"File '{os.fspath(value)}' is missing",
            )

        if not os.access(value, os.R_OK):
            raise ValueError(
                f"No access to file {path_repr(value)}",
            )

        return value

    @validator("addresses")
    def _validate_addresses(cls, value):  # noqa: N805
        if not value:
            raise ValueError("Passed empty parameter 'addresses'")
        return value

    @root_validator()
    def _validate_auth(cls, values: dict) -> dict:  # noqa: N805
        user = values.get("user", None)
        password = values.get("password", None)
        keytab = values.get("keytab", None)

        passed_user_and_keytab = user and keytab
        passed_user_and_pass = user and password

        if not user and not password and not keytab:
            # anonymous access
            return values

        if passed_user_and_keytab and not password:
            # valid credentials
            return values

        if passed_user_and_pass and not keytab:
            # valid credentials
            return values

        raise ValueError(
            "Please provide either `keytab` and `user`, or `password` and "
            "`user` for Kerberos auth, or none of parameters for anonymous auth",
        )

    @staticmethod
    def _calculate_hash(value: str):
        hash_object = hashlib.md5()  # noqa: S303, S324
        hash_object.update(value.encode("utf-8"))
        return hash_object.hexdigest()

    def _get_jaas_conf(self) -> Optional[str]:
        service_name = self._calculate_hash(
            f"{self.addresses}{self.user}{self.cluster}",
        )
        if self.password is not None:
            return self._password_jaas(service_name, self.password)

        if self.keytab is not None:
            return self._prepare_jaas_for_keytab(service_name, self.keytab)

        return None

    def _prepare_jaas_for_keytab(self, service_name, keytab: LocalPath) -> str:
        if self.deploy_keytab:
            processed_keytab = self._move_keytab(keytab)
            self.spark.sparkContext.addFile(os.fspath(keytab))
        else:
            processed_keytab = keytab

        return textwrap.dedent(
            f"""
            com.sun.security.auth.module.Krb5LoginModule required
            keyTab="{processed_keytab}"
            principal="{self.user}"
            serviceName="{service_name}"
            renewTicket=true
            storeKey=true
            useKeyTab=true
            useTicketCache=false;
            """,
        ).strip()  # removes a carriage return at the beginning and end

    def _password_jaas(self, service_name: str, password: SecretStr) -> str:
        return textwrap.dedent(
            f"""
            org.apache.kafka.common.security.plain.PlainLoginModule required
            serviceName="{service_name}"
            username="{self.user}"
            password="{password.get_secret_value()}";
            """,
        ).strip()  # removes a carriage return at the beginning and end

    @staticmethod
    def _move_keytab(keytab: LocalPath) -> LocalPath:
        cwd = LocalPath(os.getcwd())
        copy_path = LocalPath(shutil.copy2(keytab, cwd))

        return copy_path.relative_to(cwd)
