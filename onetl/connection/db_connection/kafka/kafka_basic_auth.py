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

from textwrap import dedent
from typing import TYPE_CHECKING

from pydantic import Field, SecretStr

from onetl.connection.db_connection.kafka.kafka_auth import KafkaAuth
from onetl.impl import GenericOptions

if TYPE_CHECKING:
    from onetl.connection import Kafka


class KafkaBasicAuth(KafkaAuth, GenericOptions):
    """
    A class designed to generate a connection configuration using a login and password.

    https://kafka.apache.org/documentation/#security_sasl_plain
    """

    user: str = Field(alias="username")
    password: SecretStr

    def get_jaas_conf(self) -> str:
        return dedent(
            f"""
            org.apache.kafka.common.security.plain.PlainLoginModule required
            username="{self.user}"
            password="{self.password.get_secret_value()}";""",
        ).strip()

    def get_options(self, kafka: Kafka) -> dict:
        return {
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": self.get_jaas_conf(),
        }
