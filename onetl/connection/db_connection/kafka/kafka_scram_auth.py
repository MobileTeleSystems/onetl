# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import TYPE_CHECKING, Literal

from pydantic import ConfigDict, Field, SecretStr

from onetl._util.spark import stringify
from onetl.connection.db_connection.kafka.kafka_auth import KafkaAuth
from onetl.impl import GenericOptions

if TYPE_CHECKING:
    from onetl.connection.db_connection.kafka.connection import Kafka


class KafkaScramAuth(KafkaAuth, GenericOptions):
    """
    Connect to Kafka using `sasl.mechanism="SCRAM-SHA-256"` or `sasl.mechanism="SCRAM-SHA-512"`.

    For more details see [Kafka Documentation](https://kafka.apache.org/documentation/#security_sasl_scram_clientconfig).

    !!! success "Added in 0.9.0"

    Examples
    --------

    Auth in Kafka with `SCRAM-SHA-256` mechanism:

    ```python
    from onetl.connection import Kafka

    auth = Kafka.ScramAuth(
        user="me",
        password="abc",
        digest="SHA-256",
    )
    ```
    Auth in Kafka with `SCRAM-SHA-512` mechanism and some custom SASL options passed to Kafka client config:

    ```python
    from onetl.connection import Kafka

    auth = Kafka.ScramAuth.parse(
        {
            "user": "me",
            "password": "abc",
            "digest": "SHA-512",
            # options with `sasl.login.` prefix are passed to Kafka client config as-is
            "sasl.login.class": "com.example.CustomScramLogin",
        }
    )
    ```
    """

    user: str = Field(alias="username")
    password: SecretStr
    digest: Literal["SHA-256", "SHA-512"]
    model_config = ConfigDict(
        strip_prefixes=("kafka.",),  # type: ignore[typeddict-unknown-key]
        known_options=frozenset(("sasl.login.*",)),  # type: ignore[typeddict-unknown-key]
        prohibited_options=frozenset(("sasl.mechanism", "sasl.jaas.config")),  # type: ignore[typeddict-unknown-key]
        extra="allow",
    )

    def get_jaas_conf(self) -> str:
        return (
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{self.user}" '
            f'password="{self.password.get_secret_value()}";'
        )

    def get_options(self, kafka: "Kafka") -> dict:
        result = {
            key: value
            for key, value in self.model_dump(by_alias=True, exclude_none=True).items()
            if key.startswith("sasl.")
        }
        result.update(
            {
                "sasl.mechanism": f"SCRAM-{self.digest}",
                "sasl.jaas.config": self.get_jaas_conf(),
            },
        )
        return stringify(result)

    def cleanup(self, kafka: "Kafka") -> None:
        # nothing to cleanup
        pass
