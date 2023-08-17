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

from typing import TYPE_CHECKING

from pydantic import Field, SecretStr

from onetl.connection.db_connection.kafka.kafka_auth import KafkaAuth
from onetl.impl import GenericOptions

if TYPE_CHECKING:
    from onetl.connection import Kafka


class KafkaBasicAuth(KafkaAuth, GenericOptions):
    """
    Connect to Kafka using ``sasl.mechanism="PLAIN"``.

    For more details see `Kafka Documentation <https://kafka.apache.org/documentation/#security_sasl_plain>`_.

    Examples
    --------

    Auth in Kafka with user and password:

    .. code:: python

        from onetl.connection import Kafka

        auth = Kafka.BasicAuth(
            user="some_user",
            password="abc",
        )

    """

    user: str = Field(alias="username")
    password: SecretStr

    def get_jaas_conf(self) -> str:  # noqa: WPS473
        return (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{self.user}" '
            f'password="{self.password.get_secret_value()}";'
        )

    def get_options(self, kafka: Kafka) -> dict:
        return {
            "sasl.mechanism": "PLAIN",
            "sasl.jaas.config": self.get_jaas_conf(),
        }

    def cleanup(self, kafka: Kafka) -> None:
        # nothing to cleanup
        pass
