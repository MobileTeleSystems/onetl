# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import TYPE_CHECKING

from pydantic import Field, SecretStr

from onetl.connection.db_connection.kafka.kafka_auth import KafkaAuth
from onetl.impl import GenericOptions

if TYPE_CHECKING:
    from onetl.connection.db_connection.kafka.connection import Kafka


class KafkaBasicAuth(KafkaAuth, GenericOptions):
    """
    Connect to Kafka using `sasl.mechanism="PLAIN"`.

    For more details see [Kafka Documentation](https://kafka.apache.org/documentation/#security_sasl_plain).

    !!! success "Added in 0.9.0"

    Examples
    --------

    Auth in Kafka with user and password:

    ```python
    from onetl.connection import Kafka

    auth = Kafka.BasicAuth(
        user="some_user",
        password="abc",
    )
    ```
    """

    user: str = Field(alias="username")
    password: SecretStr

    def get_jaas_conf(self) -> str:
        return (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{self.user}" '
            f'password="{self.password.get_secret_value()}";'
        )

    def get_options(self, kafka: "Kafka") -> dict:
        return {
            "sasl.mechanism": "PLAIN",
            "sasl.jaas.config": self.get_jaas_conf(),
        }

    def cleanup(self, kafka: "Kafka") -> None:
        # nothing to cleanup
        pass
