<a id="kafka-connection"></a>

# Kafka Connection

### *class* onetl.connection.db_connection.kafka.connection.Kafka(\*, spark: SparkSession, cluster: Cluster, addresses: List[str], auth: [KafkaAuth](auth.md#onetl.connection.db_connection.kafka.kafka_auth.KafkaAuth) | None = None, protocol: [KafkaProtocol](protocol.md#onetl.connection.db_connection.kafka.kafka_protocol.KafkaProtocol) = KafkaPlaintextProtocol(), extra: KafkaExtra = KafkaExtra())

This connector is designed to read and write from Kafka in batch mode.

Based on [official Kafka Source For Spark](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html).

#### SEE ALSO
Before using this connector please take into account [Prerequisites](prerequisites.md#kafka-prerequisites)

#### NOTE
This connector is for **batch** ETL processes, not streaming.

#### Versionadded
Added in version 0.9.0.

* **Parameters:**
  **addresses**
  : A list of broker addresses, for example `["192.168.1.10:9092", "192.168.1.11:9092"]`.

  **cluster**
  : Cluster name. Used for HWM and lineage.

  **auth**
  : Kafka authentication mechanism. `None` means anonymous auth.

  **protocol**
  : Kafka security protocol.

  **extra**
  : A dictionary of additional properties to be used when connecting to Kafka.
    <br/>
    These are Kafka-specific properties that control behavior of the producer or consumer. See:
    * [producer options documentation](https://kafka.apache.org/documentation/#producerconfigs)
    * [consumer options documentation](https://kafka.apache.org/documentation/#consumerconfigs)
    * [Spark Kafka documentation](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#kafka-specific-configurations)
    <br/>
    Options are passed without `kafka.` prefix, for example:
    <br/>
    For example:
    ```python
    extra = {
        "group.id": "myGroup",
        "request.timeout.ms": 120000,
    }
    ```
    <br/>
    #### WARNING
    Options that populated from connection
    attributes (like `bootstrap.servers`, `sasl.*`, `ssl.*`) are not allowed to override.

### Examples

Create Kafka connection with `PLAINTEXT` protocol and `SCRAM-SHA-256` auth

```py
from onetl.connection import Kafka
from pyspark.sql import SparkSession

# Create Spark session with Kafka connector loaded
maven_packages = Kafka.get_packages(spark_version="3.5.5")
exclude_packages = Kafka.get_exclude_packages()
spark = (
    SparkSession.builder.appName("spark-app-name")
    .config("spark.jars.packages", ",".join(maven_packages))
    .config("spark.jars.excludes", ",".join(exclude_packages))
    .getOrCreate()
)

# Create connection
kafka = Kafka(
    addresses=["mybroker:9092", "anotherbroker:9092"],
    cluster="my-cluster",
    auth=Kafka.ScramAuth(
        user="me",
        password="abc",
        digest="SHA-256",
    ),
    spark=spark,
).check()
```

Create Kafka connection with `PLAINTEXT` protocol and Kerberos (`GSSAPI`) auth

```py
# Create Spark session with Kafka connector loaded
...

# Create connection
kafka = Kafka(
    addresses=["mybroker:9092", "anotherbroker:9092"],
    cluster="my-cluster",
    auth=Kafka.KerberosAuth(
        principal="me@example.com",
        keytab="/path/to/keytab",
        deploy_keytab=True,
    ),
    spark=spark,
).check()
```

Create Kafka connection with `SASL_SSL` protocol and `SCRAM-SHA-512` auth

```py
from pathlib import Path

# Create Spark session with Kafka connector loaded
...

# Create connection
kafka = Kafka(
    addresses=["mybroker:9092", "anotherbroker:9092"],
    cluster="my-cluster",
    protocol=Kafka.SSLProtocol(
        # read client certificate and private key from file
        keystore_type="PEM",
        keystore_certificate_chain=Path("path/to/user.crt").read_text(),
        keystore_key=Path("path/to/user.key").read_text(),
        # read server public certificate from file
        truststore_type="PEM",
        truststore_certificates=Path("/path/to/server.crt").read_text(),
    ),
    auth=Kafka.ScramAuth(
        user="me",
        password="abc",
        digest="SHA-512",
    ),
    spark=spark,
).check()
```

Create Kafka connection with extra options

```py
# Create Spark session with Kafka connector loaded
...

# Create connection
kafka = Kafka(
    addresses=["mybroker:9092", "anotherbroker:9092"],
    cluster="my-cluster",
    protocol=...,
    auth=...,
    extra={"max.request.size": 1024 * 1024},  # <--
    spark=spark,
).check()
```

<!-- !! processed by numpydoc !! -->

#### check()

Check source availability. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

If not, an exception will be raised.

* **Returns:**
  Connection itself
* **Raises:**
  RuntimeError
  : If the connection is not available

### Examples

```python
connection.check()
```

<!-- !! processed by numpydoc !! -->

#### close()

Close all connections created to Kafka. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### NOTE
Connection can be used again after it was closed.

* **Returns:**
  Connection itself

### Examples

Close connection automatically:

```python
with connection:
    ...
```

Close connection manually:

```python
connection.close()
```

<!-- !! processed by numpydoc !! -->

#### *classmethod* get_exclude_packages() → list[str]

Get package names to be excluded by Spark. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

#### Versionadded
Added in version 0.13.0.

### Examples

```python
from onetl.connection import Kafka

Kafka.get_exclude_packages()
```

<!-- !! processed by numpydoc !! -->

#### *classmethod* get_packages(spark_version: str, scala_version: str | None = None) → list[str]

Get package names to be downloaded by Spark. [![support_hooks](https://img.shields.io/badge/%20-support%20hooks-blue)](https://onetl.readthedocs.io/en/0.13.5/hooks/index.html)

See [Maven package index](https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10)
for all available packages.

* **Parameters:**
  **spark_version**
  : Spark version in format `major.minor.patch`.

  **scala_version**
  : Scala version in format `major.minor`.
    <br/>
    If `None`, `spark_version` is used to determine Scala version.

### Examples

```python
from onetl.connection import Kafka

Kafka.get_packages(spark_version="3.5.5")
Kafka.get_packages(spark_version="3.5.5", scala_version="2.12")
```

<!-- !! processed by numpydoc !! -->
