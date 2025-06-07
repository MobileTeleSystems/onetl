<a id="kafka-kerberos-auth"></a>

# Kafka KerberosAuth

### *pydantic model* onetl.connection.db_connection.kafka.kafka_kerberos_auth.KafkaKerberosAuth

Connect to Kafka using `sasl.mechanism="GSSAPI"`.

For more details see:

* [Kafka Documentation](https://kafka.apache.org/documentation/#security_sasl_kerberos_clientconfig)
* [Krb5LoginModule documentation](https://docs.oracle.com/javase/8/docs/jre/api/security/jaas/spec/com/sun/security/auth/module/Krb5LoginModule.html)

#### Versionadded
Added in version 0.9.0.

### Examples

Auth in Kafka with keytab, automatically deploy keytab files to all Spark hosts (driver and executors):

```python
from onetl.connection import Kafka

auth = Kafka.KerberosAuth(
    principal="user",
    keytab="/path/to/keytab",
    deploy_keytab=True,
)
```

Auth in Kafka with keytab, keytab is **already deployed** on all Spark hosts (driver and executors):

```python
from onetl.connection import Kafka

auth = Kafka.KerberosAuth(
    principal="user",
    keytab="/path/to/keytab",
    deploy_keytab=False,
)
```

Auth in Kafka with existing Kerberos ticket (only Spark session created with `master=local`):

```python
from onetl.connection import Kafka

auth = Kafka.KerberosAuth(
    principal="user",
    use_keytab=False,
    use_ticket_cache=True,
)
```

Pass custom options for JAAS config and Kafka SASL:

```python
from onetl.connection import Kafka

auth = Kafka.KerberosAuth.parse(
    {
        "principal": "user",
        "keytab": "/path/to/keytab",
        # options without sasl.kerberos. prefix are passed to JAAS config
        # names are in camel case!
        "isInitiator": True,
        # options with `sasl.kerberos.` prefix are passed to Kafka client config as-is
        "sasl.kerberos.kinit.cmd": "/usr/bin/kinit",
    }
)
```

<!-- !! processed by numpydoc !! -->

#### *field* principal *: str* *[Required]*

#### *field* keytab *: LocalPath | None* *= None* *(alias 'keyTab')*

#### *field* deploy_keytab *: bool* *= True*

#### *field* service_name *: str* *= 'kafka'* *(alias 'serviceName')*

#### *field* renew_ticket *: bool* *= True* *(alias 'renewTicket')*

#### *field* store_key *: bool* *= True* *(alias 'storeKey')*

#### *field* use_keytab *: bool* *= True* *(alias 'useKeyTab')*

#### *field* use_ticket_cache *: bool* *= False* *(alias 'useTicketCache')*

#### get_jaas_conf(kafka: [Kafka](connection.md#onetl.connection.db_connection.kafka.connection.Kafka)) → str

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
