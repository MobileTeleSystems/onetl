<a id="kafka-ssl-protocol"></a>

# Kafka SSLProtocol

### *pydantic model* onetl.connection.db_connection.kafka.kafka_ssl_protocol.KafkaSSLProtocol

Connect to Kafka using `SSL` or `SASL_SSL` security protocols.

For more details see:

* [Kafka Documentation](https://kafka.apache.org/documentation/#producerconfigs_ssl.keystore.location)
* [IBM Documentation](https://www.ibm.com/docs/en/cloud-paks/cp-biz-automation/19.0.x?topic=fcee-kafka-using-ssl-kerberos-authentication)
* [How to use PEM Certificates with Kafka](https://codingharbour.com/apache-kafka/using-pem-certificates-with-apache-kafka/)

#### Versionadded
Added in version 0.9.0.

### Examples

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
    keystore_certificate_chain="-----BEGIN CERTIFICATE-----\nMIIDZjC...\n-----END CERTIFICATE-----",
    keystore_key="-----BEGIN PRIVATE KEY-----\nMIIEvg..\n-----END PRIVATE KEY-----",
    truststore_type="PEM",
    truststore_certificates="-----BEGIN CERTIFICATE-----\nMICC...\n-----END CERTIFICATE-----",
)
```

Pass custom options:

```python
protocol = Kafka.SSLProtocol.parse(
    {
        # Just the same options as above, but using Kafka config naming with dots
        "ssl.keystore.type": "PEM",
        "ssl.keystore.certificate_chain": "-----BEGIN CERTIFICATE-----\nMIIDZjC...\n-----END CERTIFICATE-----",
        "ssl.keystore.key": "-----BEGIN PRIVATE KEY-----\nMIIEvg..\n-----END PRIVATE KEY-----",
        "ssl.truststore.type": "PEM",
        "ssl.truststore.certificates": "-----BEGIN CERTIFICATE-----\nMICC...\n-----END CERTIFICATE-----",
        # Any option starting from "ssl." is passed to Kafka client as-is
        "ssl.protocol": "TLSv1.3",
    }
)
```

### Not recommended

These options are error-prone and have several drawbacks, so it is not recommended to use them.

Passing PEM certificates as files:

* ENCRYPT `user.key` file with password `"some password"` [using PKCS#8 scheme](https://www.mkssoftware.com/docs/man1/openssl_pkcs8.1.asp).
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

<!-- !! processed by numpydoc !! -->

#### *field* keystore_type *: str* *[Required]* *(alias 'ssl.keystore.type')*

#### *field* keystore_location *: LocalPath | None* *= None* *(alias 'ssl.keystore.location')*

#### *field* keystore_password *: SecretStr | None* *= None* *(alias 'ssl.keystore.password')*

#### *field* keystore_certificate_chain *: str | None* *= None* *(alias 'ssl.keystore.certificate.chain')*

#### *field* keystore_key *: SecretStr | None* *= None* *(alias 'ssl.keystore.key')*

#### *field* key_password *: SecretStr | None* *= None* *(alias 'ssl.key.password')*

#### *field* truststore_type *: str* *[Required]* *(alias 'ssl.truststore.type')*

#### *field* truststore_location *: LocalPath | None* *= None* *(alias 'ssl.truststore.location')*

#### *field* truststore_password *: SecretStr | None* *= None* *(alias 'ssl.truststore.password')*

#### *field* truststore_certificates *: str | None* *= None* *(alias 'ssl.truststore.certificates')*

#### get_options(kafka: [Kafka](connection.md#onetl.connection.db_connection.kafka.connection.Kafka)) → dict

Get options for Kafka connection

* **Parameters:**
  **kafka**
  : Connection instance
* **Returns:**
  dict:
  : Kafka client options

<!-- !! processed by numpydoc !! -->

#### cleanup(kafka: [Kafka](connection.md#onetl.connection.db_connection.kafka.connection.Kafka)) → None

This method is called while closing Kafka connection.

Implement it to cleanup resources like temporary files.

* **Parameters:**
  **kafka**
  : Connection instance

<!-- !! processed by numpydoc !! -->
