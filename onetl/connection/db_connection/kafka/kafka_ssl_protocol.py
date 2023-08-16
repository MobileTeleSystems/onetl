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

from pathlib import Path
from typing import TYPE_CHECKING, Optional

from pydantic import Field, SecretStr, validator

from onetl._internal import stringify
from onetl._util.file import is_file_readable
from onetl.impl import GenericOptions, LocalPath

if TYPE_CHECKING:
    from onetl.connection import Kafka

from onetl.connection.db_connection.kafka.kafka_protocol import KafkaProtocol


class KafkaSSLProtocol(KafkaProtocol, GenericOptions):
    """
    Class encapsulates all the necessary configurations and interactions for managing
    SSL protocols within a Kafka system. It serves as a handler for different keystore types and truststore configurations,
    allowing the connection to Kafka brokers through SSL.

    See:
    * `IBM Documentation <https://www.ibm.com/docs/en/cloud-paks/cp-biz-automation/19.0.x?topic=fcee-kafka-using-ssl-kerberos-authentication>`_
    * `Kafka Documentation <https://kafka.apache.org/090/documentation.html>`_
    * `How to use PEM Certificates with Kafka <https://codingharbour.com/apache-kafka/using-pem-certificates-with-apache-kafka/>`_

    Examples
    --------

    PEM certificates as raw strings:

    .. code:: python

        ssl = (
            Kafka.SSLProtocol(
                keystore_type="PEM",
                keystore_certificate_chain="-----BEGIN CERTIFICATE-----MIIDZjC...-----END CERTIFICATE-----",
                keystore_key="-----BEGIN ENCRYPTED PRIVATE KEY-----MIIDZjC..-----END ENCRYPTED PRIVATE KEY-----",
                key_password="password",
                truststore_type="PEM",
                truststore_certificates="-----BEGIN CERTIFICATE-----MICC...-----END CERTIFICATE-----",
            ),
        )

    PEM certificates as file paths:

    .. code:: python

        ssl = Kafka.SSLProtocol(
            keystore_type="PEM",
            keystore_location="/path/to/file/containing/certificate/chain.pem",
            key_password="password",
            truststore_type="PEM",
            truststore_location="/path/to/truststore/certificate.pem",
        )

    JKS store path:

    .. code:: python

        ssl = Kafka.SSLProtocol(
            keystore_type="JKS",
            keystore_location="/opt/keystore.jks",
            keystore_password="password",
            truststore_type="JKS",
            truststore_location="/opt/truststore.jks",
            truststore_password="password",
        )

    Custom options:

    .. code:: python

        ssl = Kafka.SSLProtocol.parse(
            {
                "ssl.keystore.type": "JKS",
                "ssl.keystore.location": "/opt/keystore.jks",
                "ssl.keystore.password": "password",
                "ssl.protocol": "TLSv1.3",
            }
        )
    """

    keystore_type: str = Field(alias="ssl.keystore.type")
    keystore_location: Optional[LocalPath] = Field(default=None, alias="ssl.keystore.location")
    keystore_password: Optional[SecretStr] = Field(default=None, alias="ssl.keystore.password")
    keystore_certificate_chain: Optional[str] = Field(default=None, alias="ssl.keystore.certificate.chain", repr=False)
    keystore_key: Optional[SecretStr] = Field(default=None, alias="ssl.keystore.key")
    # https://knowledge.informatica.com/s/article/145442?language=en_US
    key_password: Optional[SecretStr] = Field(default=None, alias="ssl.key.password")
    truststore_type: str = Field(alias="ssl.truststore.type")
    truststore_location: Optional[LocalPath] = Field(default=None, alias="ssl.truststore.location")
    truststore_password: Optional[SecretStr] = Field(default=None, alias="ssl.truststore.password")
    truststore_certificates: Optional[str] = Field(default=None, alias="ssl.truststore.certificates", repr=False)

    class Config:
        known_options = {"ssl.*"}
        strip_prefixes = ["kafka."]
        extra = "allow"

    def get_options(self, kafka: Kafka) -> dict:
        result = self.dict(by_alias=True, exclude_none=True)
        if kafka.auth:
            result["security.protocol"] = "SASL_SSL"
        else:
            result["security.protocol"] = "SSL"
        return stringify(result)

    def cleanup(self, kafka: Kafka) -> None:
        # nothing to cleanup
        pass

    @validator("keystore_location", "truststore_location")
    def validate_path(cls, value: LocalPath) -> Path:
        return is_file_readable(value)
