<a id="kafka-auth"></a>

# Kafka Auth

### *class* onetl.connection.db_connection.kafka.kafka_auth.KafkaAuth

Interface for Kafka connection Auth classes.

#### Versionadded
Added in version 0.9.0.

<!-- !! processed by numpydoc !! -->

#### *abstract* get_options(kafka: [Kafka](connection.md#onetl.connection.db_connection.kafka.connection.Kafka)) → dict

Get options for Kafka connection

* **Parameters:**
  **kafka**
  : Connection instance
* **Returns:**
  dict:
  : Kafka client options

<!-- !! processed by numpydoc !! -->

#### *abstract* cleanup(kafka: [Kafka](connection.md#onetl.connection.db_connection.kafka.connection.Kafka)) → None

This method is called while closing Kafka connection.

Implement it to cleanup resources like temporary files.

* **Parameters:**
  **kafka**
  : Connection instance

<!-- !! processed by numpydoc !! -->
