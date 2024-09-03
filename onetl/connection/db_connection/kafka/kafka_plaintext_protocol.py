# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
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

    .. versionadded:: 0.9.0

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
