# SPDX-FileCopyrightText: 2023-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from typing import TYPE_CHECKING

try:
    from pydantic.v1 import Field, SecretStr, validator
except (ImportError, AttributeError):
    from pydantic import Field, SecretStr, validator  # type: ignore[no-redef, assignment]

from onetl._util.alias import avoid_alias
from onetl._util.file import readable_local_file
from onetl._util.spark import stringify
from onetl.connection.db_connection.kafka.kafka_protocol import KafkaProtocol
from onetl.impl import GenericOptions, LocalPath

if TYPE_CHECKING:
    from onetl.connection.db_connection.kafka.connection import Kafka


class KafkaSSLProtocol(KafkaProtocol, GenericOptions):
    """
    Connect to Kafka using `SSL` or `SASL_SSL` security protocols.

    For more details see:

    * [Kafka Documentation](https://kafka.apache.org/documentation/#producerconfigs_ssl.keystore.location)
    * [IBM Documentation](https://www.ibm.com/docs/en/cloud-paks/cp-biz-automation/19.0.x?topic=fcee-kafka-using-ssl-kerberos-authentication)
    * [How to use PEM Certificates with Kafka](https://codingharbour.com/apache-kafka/using-pem-certificates-with-apache-kafka/)

    !!! success "Added in 0.9.0"

    Examples
    --------

    === "TLS (verify only server public certificate)"

        Pass PEM certificate as files located on Spark driver host:

        ```python
        from pathlib import Path

        # Just read existing files located on host, and pass key and certificates as strings
        protocol = Kafka.SSLProtocol(
            truststore_type="PEM",
            truststore_certificates=Path("/path/to/server.crt").read_text(),
        )
        ```
        Pass PEM certificate as raw string:

        ```python
        protocol = Kafka.SSLProtocol(
            truststore_type="PEM",
            truststore_certificates="-----BEGIN CERTIFICATE...\\n...END CERTIFICATE-----",
        )
        ```

    === "mTLS (mutual certificate check of client and server)"

        Pass PEM key and certificates as files located on Spark driver host:

        ```python
        from pathlib import Path

        # Just read existing files located on host, and pass key and certificates as strings
        protocol = Kafka.SSLProtocol(
            keystore_type="PEM",
            keystore_certificate_chain=Path("path/to/user.crt").read_text(),
            keystore_key=Path("path/to/user.key").read_text(),
            truststore_type="PEM",
            truststore_certificates=Path("/path/to/server.crt").read_text(),
        )
        ```
        Pass PEM key and certificates as raw strings:

        ```python
        protocol = Kafka.SSLProtocol(
            keystore_type="PEM",
            keystore_certificate_chain="-----BEGIN CERTIFICATE...\\n...END CERTIFICATE-----",
            keystore_key="-----BEGIN PRIVATE KEY...\\n...END PRIVATE KEY-----",
            truststore_type="PEM",
            truststore_certificates="-----BEGIN CERTIFICATE...\\n...END CERTIFICATE-----",
        )
        ```

    === "Custom Kafka client options"

        ```python
        protocol = Kafka.SSLProtocol.parse(
            {
                # Just the same options as above, but using Kafka config naming with dots
                "ssl.keystore.type": "PEM",
                "ssl.keystore.certificate_chain": "-----BEGIN CERTIFICATE...\\n...END CERTIFICATE-----",
                "ssl.keystore.key": "-----BEGIN PRIVATE KEY...\\n...END PRIVATE KEY-----",
                "ssl.truststore.type": "PEM",
                "ssl.truststore.certificates": "-----BEGIN CERTIFICATE...\\n...END CERTIFICATE-----",
                # Any option starting from "ssl." is passed to Kafka client as-is
                "ssl.protocol": "TLSv1.3",
            }
        )
        ```
    ??? note "Not recommended"

        These options are error-prone and have several drawbacks, so it is not recommended to use them.

        Passing PEM certificates as files:

        * ENCRYPT `user.key` file with password `"some password"` [using
          PKCS#8 scheme](https://www.mkssoftware.com/docs/man1/openssl_pkcs8.1.asp).
        * Save encrypted key to file `/path/to/user/encrypted_key_with_certificate_chain.pem`.
        * Then append user certificate to the end of this file.
        * Deploy this file (and server certificate too) to **EVERY** host Spark could run (both driver and executors).
        * Then pass file locations and password for key decryption to options below.

        ```python
        protocol = Kafka.SSLProtocol(
            keystore_type="PEM",
            keystore_location="/path/to/user/encrypted_key_with_certificate_chain.pem",
            key_password="some password",
            truststore_type="PEM",
            truststore_location="/path/to/server.crt",
        )
        ```
        Passing JKS (Java Key Store) location:

        * [Add user key and certificate to JKS keystore](https://stackoverflow.com/a/4326346).
        * [Add server certificate to JKS truststore](https://stackoverflow.com/a/373307).
        * This should be done on **EVERY** host Spark could run (both driver and executors).
        * Pass keystore and truststore paths to options below, as well as passwords for accessing these stores:

        ```python
        protocol = Kafka.SSLProtocol(
            keystore_type="JKS",
            keystore_location="/usr/lib/jvm/default/lib/security/keystore.jks",
            keystore_password="changeit",
            truststore_type="JKS",
            truststore_location="/usr/lib/jvm/default/lib/security/truststore.jks",
            truststore_password="changeit",
        )
        ```
    """

    keystore_type: str | None = Field(  # type: ignore[literal-required]
        default=None,
        alias=avoid_alias("ssl.keystore.type"),
    )
    keystore_location: LocalPath | None = Field(  # type: ignore[literal-required]
        default=None,
        alias=avoid_alias("ssl.keystore.location"),
    )
    keystore_password: SecretStr | None = Field(  # type: ignore[literal-required]
        default=None,
        alias=avoid_alias("ssl.keystore.password"),
    )
    keystore_certificate_chain: str | None = Field(  # type: ignore[literal-required]
        default=None,
        alias=avoid_alias("ssl.keystore.certificate.chain"),
        repr=False,
    )
    keystore_key: SecretStr | None = Field(  # type: ignore[literal-required]
        default=None,
        alias=avoid_alias("ssl.keystore.key"),
    )

    # https://knowledge.informatica.com/s/article/145442?language=en_US
    key_password: SecretStr | None = Field(  # type: ignore[literal-required]
        default=None,
        alias=avoid_alias("ssl.key.password"),
    )
    truststore_type: str = Field(alias=avoid_alias("ssl.truststore.type"))  # type: ignore[literal-required]
    truststore_location: LocalPath | None = Field(  # type: ignore[literal-required]
        default=None,
        alias=avoid_alias("ssl.truststore.location"),
    )
    truststore_password: SecretStr | None = Field(  # type: ignore[literal-required]
        default=None,
        alias=avoid_alias("ssl.truststore.password"),
    )
    truststore_certificates: str | None = Field(  # type: ignore[literal-required]
        default=None,
        alias=avoid_alias("ssl.truststore.certificates"),
        repr=False,
    )

    class Config:
        known_options = frozenset(("ssl.*",))
        strip_prefixes = ("kafka.",)
        extra = "allow"

    def get_options(self, kafka: "Kafka") -> dict:
        result = self.dict(by_alias=True, exclude_none=True)
        if kafka.auth:
            result["security.protocol"] = "SASL_SSL"
        else:
            result["security.protocol"] = "SSL"
        return stringify(result)

    def cleanup(self, kafka: "Kafka") -> None:
        # nothing to cleanup
        pass

    @validator("keystore_location", "truststore_location", pre=True)
    def validate_path(cls, value: LocalPath) -> Path:
        return readable_local_file(LocalPath(value).expanduser().resolve())
