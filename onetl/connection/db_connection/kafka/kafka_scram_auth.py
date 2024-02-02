# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import Field, SecretStr
from typing_extensions import Literal

from onetl._internal import stringify
from onetl.connection.db_connection.kafka.kafka_auth import KafkaAuth
from onetl.impl import GenericOptions

if TYPE_CHECKING:
    from onetl.connection import Kafka


class KafkaScramAuth(KafkaAuth, GenericOptions):
    """
    Connect to Kafka using ``sasl.mechanism="SCRAM-SHA-*"``.

    For more details see `Kafka Documentation <https://kafka.apache.org/documentation/#security_sasl_scram_clientconfig>`_.

    Examples
    --------

    Auth in Kafka with ``SCRAM-SHA-256`` mechanism:

    .. code:: python

        from onetl.connection import Kafka

        auth = Kafka.ScramAuth(
            user="me",
            password="abc",
            digest="SHA-256",
        )

    Auth in Kafka with ``SCRAM-SHA-512`` mechanism and some custom SASL options passed to Kafka client config:

    .. code:: python

        from onetl.connection import Kafka

        auth = Kafka.ScramAuth.parse(
            {
                "user": "me",
                "password": "abc",
                "digest": "SHA-512",
                # options with sasl.login. prefix are passed to Kafka client config
                "sasl.login.class": "com.example.CustomScramLogin",
            }
        )
    """

    user: str = Field(alias="username")
    password: SecretStr
    digest: Literal["SHA-256", "SHA-512"]

    class Config:
        strip_prefixes = ["kafka."]
        # https://kafka.apache.org/documentation/#producerconfigs_sasl.login.class
        known_options = {"sasl.login.*"}
        prohibited_options = {"sasl.mechanism", "sasl.jaas.config"}
        extra = "allow"

    def get_jaas_conf(self) -> str:  # noqa: WPS473
        return (
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{self.user}" '
            f'password="{self.password.get_secret_value()}";'
        )

    def get_options(self, kafka: Kafka) -> dict:
        result = {
            key: value for key, value in self.dict(by_alias=True, exclude_none=True).items() if key.startswith("sasl.")
        }
        result.update(
            {
                "sasl.mechanism": f"SCRAM-{self.digest}",
                "sasl.jaas.config": self.get_jaas_conf(),
            },
        )
        return stringify(result)

    def cleanup(self, kafka: Kafka) -> None:
        # nothing to cleanup
        pass
