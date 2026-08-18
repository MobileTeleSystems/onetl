# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0

from typing import TYPE_CHECKING

from onetl.connection.db_connection.kafka.kafka_protocol import KafkaProtocol
from onetl.impl import FrozenModel

if TYPE_CHECKING:
    from onetl.connection.db_connection.kafka.connection import Kafka


class KafkaPlaintextProtocol(KafkaProtocol, FrozenModel):
    """
    Connect to Kafka using `PLAINTEXT` or `SASL_PLAINTEXT` security protocols.

    !!! warning

        Not recommended to use on production environments.
        Prefer [SSLProtocol][onetl.connection.db_connection.kafka.kafka_ssl_protocol.KafkaSSLProtocol].

    !!! success "Added in 0.9.0"

    Examples
    --------

    ```python
    # No options
    protocol = Kafka.PlaintextProtocol()
    ```
    """

    def get_options(self, kafka: "Kafka") -> dict:
        # Access to Kafka is needed to select the type of protocol depending on the authentication scheme.
        if kafka.auth:
            return {"security.protocol": "SASL_PLAINTEXT"}
        return {"security.protocol": "PLAINTEXT"}

    def cleanup(self, kafka: "Kafka") -> None:
        # nothing to cleanup
        pass
