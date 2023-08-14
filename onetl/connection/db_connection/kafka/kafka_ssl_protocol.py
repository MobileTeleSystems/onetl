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

import os
from typing import TYPE_CHECKING, Optional

from pydantic import Field, SecretStr, validator

from onetl.impl import GenericOptions, LocalPath

if TYPE_CHECKING:
    from onetl.connection import Kafka

from onetl.connection.db_connection.kafka.kafka_protocol import KafkaProtocol


class KafkaSSLProtocol(KafkaProtocol, GenericOptions):
    """
    See:
    * `IBM Documentation <https://www.ibm.com/docs/en/cloud-paks/cp-biz-automation/19.0.x?topic=fcee-kafka-using-ssl-kerberos-authentication>`_
    * `Kafka Documentation <https://kafka.apache.org/090/documentation.html>`_
    * `How to use PEM Certificates with Kafka <https://codingharbour.com/apache-kafka/using-pem-certificates-with-apache-kafka/>`_

    Examples
    --------

    PEM certificates as raw strings:

    .. code:: python

        kafka = Kafka(
            protocol=Kafka.SSLProtocol(
                keystore_type="PEM",
                keystore_certificate_chain="<certificate-chain-here>",
                keystore_key="<private-key_string>",
                key_password="<private_key_password>",
                truststore_type="PEM",
                truststore_certificates="<trusted-certificates>",
            ),
        )

    PEM certificates as file paths:

    .. code:: python

        ssl = Kafka.SSLProtocol(
            keystore_type="PEM",
            keystore_location="/path/to/file/containing/certificate/chain.pem",
            key_password="<private_key_password>",
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
                "keystore_type": "JKS",
                "keystore_location": "/opt/keystore.jks",
                "keystore_password": "password",
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
        return result

    def cleanup(self, kafka: Kafka) -> None:
        # nothing to cleanup
        pass

    @validator("keystore_location", "truststore_location")
    def validate_path(cls, value: LocalPath) -> LocalPath:
        if value and not os.path.exists(value):
            raise ValueError(f"The path {value} does not exist")
        if value and not os.access(value, os.R_OK):
            raise ValueError(f"The path {value} is not readable")
        return value
