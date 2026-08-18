# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from onetl.connection.db_connection.kafka.connection import Kafka
from onetl.connection.db_connection.kafka.dialect import KafkaDialect
from onetl.connection.db_connection.kafka.extra import KafkaExtra
from onetl.connection.db_connection.kafka.kafka_auth import KafkaAuth
from onetl.connection.db_connection.kafka.kafka_basic_auth import KafkaBasicAuth
from onetl.connection.db_connection.kafka.kafka_kerberos_auth import KafkaKerberosAuth
from onetl.connection.db_connection.kafka.kafka_oauth2_client_credentials import (
    KafkaOAuth2ClientCredentials,
)
from onetl.connection.db_connection.kafka.kafka_plaintext_protocol import KafkaPlaintextProtocol
from onetl.connection.db_connection.kafka.kafka_protocol import KafkaProtocol
from onetl.connection.db_connection.kafka.kafka_scram_auth import KafkaScramAuth
from onetl.connection.db_connection.kafka.kafka_ssl_protocol import KafkaSSLProtocol
from onetl.connection.db_connection.kafka.options import (
    KafkaReadOptions,
    KafkaTopicExistBehaviorKafka,
    KafkaWriteOptions,
)
from onetl.connection.db_connection.kafka.slots import KafkaSlots

__all__ = [
    "Kafka",
    "KafkaAuth",
    "KafkaBasicAuth",
    "KafkaDialect",
    "KafkaExtra",
    "KafkaKerberosAuth",
    "KafkaOAuth2ClientCredentials",
    "KafkaPlaintextProtocol",
    "KafkaProtocol",
    "KafkaReadOptions",
    "KafkaSSLProtocol",
    "KafkaScramAuth",
    "KafkaSlots",
    "KafkaTopicExistBehaviorKafka",
    "KafkaWriteOptions",
]
