<a id="kafka-basic-auth"></a>

# Kafka BasicAuth

### *pydantic model* onetl.connection.db_connection.kafka.kafka_basic_auth.KafkaBasicAuth

Connect to Kafka using `sasl.mechanism="PLAIN"`.

For more details see [Kafka Documentation](https://kafka.apache.org/documentation/#security_sasl_plain).

#### Versionadded
Added in version 0.9.0.

### Examples

Auth in Kafka with user and password:

```python
from onetl.connection import Kafka

auth = Kafka.BasicAuth(
    user="some_user",
    password="abc",
)
```

<!-- !! processed by numpydoc !! -->

#### *field* user *: str* *[Required]* *(alias 'username')*

#### *field* password *: SecretStr* *[Required]*

#### get_jaas_conf() → str

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
