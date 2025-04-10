<a id="kafka-plaintext-protocol"></a>

# Kafka PlaintextProtocol

### *pydantic model* onetl.connection.db_connection.kafka.kafka_plaintext_protocol.KafkaPlaintextProtocol

Connect to Kafka using `PLAINTEXT` or `SASL_PLAINTEXT` security protocols.

#### WARNING
Not recommended to use on production environments.
Prefer [`SSLProtocol`](ssl_protocol.md#onetl.connection.db_connection.kafka.kafka_ssl_protocol.KafkaSSLProtocol).

#### Versionadded
Added in version 0.9.0.

### Examples

```python
# No options
protocol = Kafka.PlaintextProtocol()
```

<!-- !! processed by numpydoc !! -->

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
