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

from onetl.impl.frozen_model import FrozenModel

if TYPE_CHECKING:
    from onetl.connection import Kafka

from onetl.connection.db_connection.kafka.kafka_protocol import KafkaProtocol


class KafkaPlaintextProtocol(KafkaProtocol, FrozenModel):
    """
    Connect to Kafka using ``PLAINTEXT`` or ``SASL_PLAINTEXT`` security protocols.

    .. warning::

        Not recommended to use on production environments.
        Prefer :obj:`SSLProtocol <onetl.connection.db_connection.kafka.kafka_ssl_protocol.KafkaSSLProtocol>`.

    Examples
    --------

    .. code:: python

        # No options
        protocol = Kafka.PlaintextProtocol()
    """

    def get_options(self, kafka: Kafka) -> dict:
        # Access to Kafka is needed to select the type of protocol depending on the authentication scheme.
        if kafka.auth:
            return {"security.protocol": "SASL_PLAINTEXT"}
        return {"security.protocol": "PLAINTEXT"}

    def cleanup(self, kafka: Kafka) -> None:
        # nothing to cleanup
        pass
