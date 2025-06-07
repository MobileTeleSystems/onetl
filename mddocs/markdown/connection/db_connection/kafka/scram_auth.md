<a id="kafka-scram-auth"></a>

# Kafka ScramAuth

### *pydantic model* onetl.connection.db_connection.kafka.kafka_scram_auth.KafkaScramAuth

Connect to Kafka using `sasl.mechanism="SCRAM-SHA-256"` or `sasl.mechanism="SCRAM-SHA-512"`.

For more details see [Kafka Documentation](https://kafka.apache.org/documentation/#security_sasl_scram_clientconfig).

#### Versionadded
Added in version 0.9.0.

### Examples

Auth in Kafka with `SCRAM-SHA-256` mechanism:

```python
from onetl.connection import Kafka

auth = Kafka.ScramAuth(
    user="me",
    password="abc",
    digest="SHA-256",
)
```

Auth in Kafka with `SCRAM-SHA-512` mechanism and some custom SASL options passed to Kafka client config:

```python
from onetl.connection import Kafka

auth = Kafka.ScramAuth.parse(
    {
        "user": "me",
        "password": "abc",
        "digest": "SHA-512",
        # options with `sasl.login.` prefix are passed to Kafka client config as-is
        "sasl.login.class": "com.example.CustomScramLogin",
    }
)
```

<!-- !! processed by numpydoc !! -->

#### *field* user *: str* *[Required]* *(alias 'username')*

#### *field* password *: SecretStr* *[Required]*

#### *field* digest *: Literal['SHA-256', 'SHA-512']* *[Required]*

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
