# Prerequisites { #kafka-prerequisites }

## Version Compatibility

- Kafka server versions:
    - Officially declared: 0.10 or higher
    - Actually tested: 3.2.3, 3.9.0 (only Kafka 3.x supports message headers)
- Spark versions: 2.4.x - 3.5.x
- Java versions: 8 - 17

See [official documentation](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html).

## Installing PySpark

To use Kafka connector you should have PySpark installed (or injected to `sys.path`) BEFORE creating the connector instance.

See [installation instruction][install-spark] for more details.

## Connecting to Kafka

### Connection address

Kafka is a distributed service, and usually has a list of brokers you can connect to (unlike other connectors, there only one host+port can be set).
Please contact your Kafka administrator to get addresses of these brokers, as there are no defaults.

Also Kafka has a feature called *advertised listeners* - client connects to one broker, and received list of other brokers in the clusters.
So you don't have to pass all brokers to `addresses`, it can be some subset. Other broker addresses will be fetched directly from the cluster.

### Connection protocol

Kafka can support different connection protocols. List of currently supported protocols:

- [PLAINTEXT][onetl.connection.db_connection.kafka.kafka_plaintext_protocol.KafkaPlaintextProtocol] (not secure)
- [SSL][onetl.connection.db_connection.kafka.kafka_ssl_protocol.KafkaSSLProtocol] (secure, recommended)

Note that specific port can listen for only one of these protocols, so it is important to set proper port number + protocol combination.

### Authentication mechanism

Kafka can support different authentication mechanism (also known as [SASL](https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer)).

List of currently supported mechanisms:

- [PLAIN][onetl.connection.db_connection.kafka.kafka_basic_auth.KafkaBasicAuth]. To no confuse this with `PLAINTEXT`connection protocol, onETL uses name `BasicAuth`.
- [GSSAPI][onetl.connection.db_connection.kafka.kafka_kerberos_auth.KafkaKerberosAuth]. To simplify naming, onETL uses name`KerberosAuth`.
- [SCRAM-SHA-256 or SCRAM-SHA-512][onetl.connection.db_connection.kafka.kafka_scram_auth.KafkaScramAuth] (recommended).

Different mechanisms use different types of credentials (login + password, keytab file, and so on).

Note that connection protocol and auth mechanism are set in pairs:

- If you see `SASL_PLAINTEXT` this means `PLAINTEXT` connection protocol + some auth mechanism.
- If you see `SASL_SSL` this means `SSL` connection protocol + some auth mechanism.
- If you see just `PLAINTEXT` or `SSL` (**no** `SASL`), this means that authentication is disabled (anonymous access).

Please contact your Kafka administrator to get details about enabled auth mechanism in a specific Kafka instance.

### Required grants

Ask your Kafka administrator to set following grants for a user, *if Kafka instance uses ACL*:

- `Describe` + `Read` for reading data from Kafka (Consumer).
- `Describe` + `Write` for writing data from Kafka (Producer).

More details can be found in [documentation](https://kafka.apache.org/documentation/#operations_in_kafka).
