import logging
import os
import re
from pathlib import Path

import pytest

from onetl.connection import Kafka
from onetl.connection.db_connection.kafka.extra import KafkaExtra
from onetl.connection.db_connection.kafka.options import KafkaTopicExistBehaviorKafka
from onetl.exception import NotAFileError
from onetl.hooks import hook

pytestmark = [pytest.mark.kafka, pytest.mark.db_connection, pytest.mark.connection]


@pytest.fixture
def create_temp_file(tmp_path_factory):
    path = Path(tmp_path_factory.mktemp("data") / "some.key")
    path.write_text("-----BEGIN CERTIFICATE-----\nFAKE_CERTIFICATE_CONTENT\n-----END CERTIFICATE-----\n")

    return path


@pytest.mark.parametrize(
    "spark_version, scala_version, package",
    [
        ("2.4.0", None, "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0"),
        ("2.4.0", "2.11", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0"),
        ("2.4.0", "2.12", "org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.0"),
        ("3.3.0", None, "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"),
        ("3.3.0", "2.12", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"),
        ("3.3.0", "2.13", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.3.0"),
    ],
)
def test_kafka_get_packages(spark_version, scala_version, package):
    assert Kafka.get_packages(spark_version=spark_version, scala_version=scala_version) == [package]


@pytest.mark.parametrize(
    "spark_version",
    [
        "2.2.0",
        "2.3.1",
    ],
)
def test_kafka_get_packages_error(spark_version):
    with pytest.raises(ValueError, match=f"Spark version must be at least 2.4, got {spark_version}"):
        Kafka.get_packages(spark_version=spark_version)


def test_kafka_too_old_spark_error(spark_mock, mocker):
    msg = "Spark version must be at least 2.4, got 2.3.1"
    mocker.patch.object(spark_mock, "version", new="2.3.1")
    with pytest.raises(ValueError, match=msg):
        Kafka(
            cluster="some_cluster",
            addresses=["192.168.1.1"],
            spark=spark_mock,
        )


def test_kafka_missing_package(spark_no_packages):
    msg = "Cannot import Java class 'org.apache.spark.sql.kafka010.KafkaSourceProvider'"
    with pytest.raises(ValueError, match=msg):
        Kafka(
            cluster="some_cluster",
            addresses=["192.168.1.1"],
            spark=spark_no_packages,
        )


@pytest.mark.parametrize(
    "option, value",
    [
        ("assign", "assign_value"),
        ("subscribe", "subscribe_value"),
        ("subscribePattern", "subscribePattern_value"),
        ("startingOffsets", "startingOffsets_value"),
        ("startingOffsetsByTimestamp", "startingOffsetsByTimestamp_value"),
        ("startingTimestamp", "startingTimestamp_value"),
        ("endingTimestamp", "endingTimestamp_value"),
        ("endingOffsets", "endingOffsets_value"),
        ("endingOffsetsByTimestamp", "endingOffsetsByTimestamp_value"),
        ("kafka.bootstrap.servers", "kafka.bootstrap.servers_value"),
        ("kafka.group.id", "kafka.group.id_value"),
        ("topic", "topic_value"),
    ],
)
@pytest.mark.parametrize(
    "options_class, class_name",
    [
        (Kafka.ReadOptions, "KafkaReadOptions"),
        (Kafka.WriteOptions, "KafkaWriteOptions"),
    ],
)
def test_kafka_options_prohibited(option, value, options_class, class_name):
    error_msg = rf"Options \['{option}'\] are not allowed to use in a {class_name}"
    with pytest.raises(ValueError, match=error_msg):
        options_class.parse({option: value})


@pytest.mark.parametrize(
    "options_class, class_name",
    [
        (Kafka.ReadOptions, "KafkaReadOptions"),
        (Kafka.WriteOptions, "KafkaWriteOptions"),
    ],
)
def test_kafka_options_unknown(caplog, options_class, class_name):
    with caplog.at_level(logging.WARNING):
        options = options_class(unknown="abc")
        assert options.unknown == "abc"

    assert f"Options ['unknown'] are not known by {class_name}, are you sure they are valid?" in caplog.text


@pytest.mark.parametrize(
    "option, value",
    [
        ("failOnDataLoss", "false"),
        ("kafkaConsumer.pollTimeoutMs", "30000"),
        ("fetchOffset.numRetries", "3"),
        ("fetchOffset.retryIntervalMs", "1000"),
        ("maxOffsetsPerTrigger", "1000"),
        ("minOffsetsPerTrigger", "500"),
        ("maxTriggerDelay", "2000"),
        ("minPartitions", "2"),
        ("groupIdPrefix", "testPrefix"),
    ],
)
def test_kafka_read_options_allowed(option, value):
    options = Kafka.ReadOptions.parse({option: value})
    assert getattr(options, option) == value


@pytest.mark.parametrize("value", [True, False])
@pytest.mark.parametrize("options_class", [Kafka.ReadOptions, Kafka.WriteOptions])
def test_kafka_options_include_headers(options_class, value):
    options = options_class(includeHeaders=value)
    assert options.include_headers == value


def test_kafka_basic_auth_get_jaas_conf(spark_mock):
    conn = Kafka(
        spark=spark_mock,
        cluster="some_cluster",
        addresses=["192.168.1.1"],
        auth=Kafka.BasicAuth(
            user="user",
            password="passwd",
        ),
    )

    assert conn.auth.user == "user"
    assert conn.cluster == "some_cluster"
    assert conn.auth.password != "passwd"
    assert conn.auth.password.get_secret_value() == "passwd"
    assert conn.addresses == ["192.168.1.1"]

    assert conn.instance_url == "kafka://some_cluster"


def test_kafka_anon_auth(spark_mock):
    conn = Kafka(
        spark=spark_mock,
        cluster="some_cluster",
        addresses=["192.168.1.1"],
    )
    assert not conn.auth
    assert conn.cluster == "some_cluster"
    assert conn.addresses == ["192.168.1.1"]

    assert conn.instance_url == "kafka://some_cluster"


@pytest.mark.parametrize("digest", ["SHA-256", "SHA-512"])
def test_kafka_scram_auth(spark_mock, digest):
    conn = Kafka(
        spark=spark_mock,
        cluster="some_cluster",
        addresses=["192.168.1.1"],
        auth=Kafka.ScramAuth(
            user="user",
            password="passwd",
            digest=digest,
        ),
    )

    assert conn.auth.user == "user"
    assert conn.cluster == "some_cluster"
    assert conn.auth.password != "passwd"
    assert conn.auth.password.get_secret_value() == "passwd"
    assert conn.auth.digest == digest
    assert conn.addresses == ["192.168.1.1"]

    assert conn.instance_url == "kafka://some_cluster"


def test_kafka_auth_keytab(spark_mock, create_keytab):
    conn = Kafka(
        spark=spark_mock,
        cluster="some_cluster",
        addresses=["192.168.1.1"],
        auth=Kafka.KerberosAuth(
            principal="user",
            keytab=create_keytab,
        ),
    )

    assert conn.auth.principal == "user"
    assert conn.cluster == "some_cluster"
    assert conn.addresses == ["192.168.1.1"]

    assert conn.instance_url == "kafka://some_cluster"


def test_kafka_empty_addresses(spark_mock):
    with pytest.raises(
        ValueError,
        match=re.escape("Passed empty parameter 'addresses'"),
    ):
        Kafka(
            spark=spark_mock,
            password="passwd",
            user="user",
            cluster="some_cluster",
            addresses=[],
        )


def test_kafka_empty_cluster(spark_mock):
    with pytest.raises(
        ValueError,
        match=re.escape(
            "cluster\n  field required (type=value_error.missing)",
        ),
    ):
        Kafka(
            spark=spark_mock,
            addresses=["192.168.1.1"],
            auth=Kafka.BasicAuth(
                password="passwd",
                user="user",
            ),
        )


@pytest.mark.parametrize(
    "option, value",
    [
        ("bootstrap.servers", "kafka.bootstrap.servers_value"),
        ("security.protocol", "ssl"),
        ("sasl.mechanism", "PLAIN"),
        ("auto.offset.reset", "true"),
        ("enable.auto.commit", "true"),
        ("interceptor.classes", "some.Class"),
        ("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"),
        ("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"),
        ("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"),
        ("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer"),
    ],
)
def test_kafka_invalid_extras(option, value):
    msg = re.escape("are not allowed to use in a KafkaExtra")
    with pytest.raises(ValueError, match=msg):
        KafkaExtra.parse({option: value})
    with pytest.raises(ValueError, match=msg):
        KafkaExtra.parse({"kafka." + option: value})


@pytest.mark.parametrize(
    "option, value",
    [
        ("kafka.group.id", "group_id"),
        ("group.id", "group_id"),
    ],
)
def test_kafka_valid_extras(option, value):
    extra_dict = KafkaExtra.parse({option: value}).dict()
    assert extra_dict["group.id"] == value


def test_kafka_kerberos_auth_not_enough_permissions_keytab_error(create_keytab):
    os.chmod(create_keytab, 0o000)  # noqa: S103, WPS339

    with pytest.raises(
        OSError,
        match=re.escape(f"No read access to file '{create_keytab}'"),
    ):
        Kafka.KerberosAuth(
            principal="user",
            keytab=create_keytab,
        )


def test_kafka_kerberos_auth_missing_keytab_error():
    path = Path("some/path").resolve()

    with pytest.raises(
        FileNotFoundError,
        match=re.escape(f"File '{path}' does not exist"),
    ):
        Kafka.KerberosAuth(
            principal="user",
            keytab="some/path",
        )


def test_kafka_kerberos_auth_wrong_keytab_type_error(tmp_path_factory):
    path = tmp_path_factory.mktemp("data")

    with pytest.raises(
        NotAFileError,
        match=f"'{path}'.* is not a file",
    ):
        Kafka.KerberosAuth(
            principal="user",
            keytab=path,
        )


def test_kafka_kerberos_auth_use_keytab_without_keytab():
    with pytest.raises(
        ValueError,
        match="keytab is required if useKeytab is True",
    ):
        Kafka.KerberosAuth(principal="user")


@pytest.mark.parametrize("option", ["sasl.kerberos.service.name", "sasl.jaas.config", "sasl.mechanism"])
def test_kafka_kerberos_auth_prohibited_options(option, create_keytab):
    msg = rf"Options \['{option}'\] are not allowed to use in a KafkaKerberosAuth"
    with pytest.raises(ValueError, match=msg):
        Kafka.KerberosAuth.parse({"principal": "user", "keytab": create_keytab, option: "value"})


@pytest.mark.parametrize("option", ["unknown", "sasl.unknown"])
def test_kafka_kerberos_auth_unknown_options(option, caplog, create_keytab):
    with caplog.at_level(logging.WARNING):
        Kafka.KerberosAuth.parse({"principal": "user", "keytab": create_keytab, option: "value"})

    assert f"Options ['{option}'] are not known by KafkaKerberosAuth, are you sure they are valid?" in caplog.text


def test_kafka_basic_auth(spark_mock):
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        auth=Kafka.BasicAuth(username="user", password="password"),
    )

    conf = kafka.auth.get_options(kafka)
    with kafka:
        assert conf == {
            "sasl.mechanism": "PLAIN",
            "sasl.jaas.config": (
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                'username="user" '
                'password="password";'
            ),
        }


@pytest.mark.parametrize("option", ["sasl.jaas.config", "sasl.mechanism"])
def test_kafka_scram_auth_prohibited_options(option):
    msg = rf"Options \['{option}'\] are not allowed to use in a KafkaScramAuth"
    with pytest.raises(ValueError, match=msg):
        Kafka.ScramAuth.parse({"username": "user", "password": "some", "digest": "SHA-256", option: "value"})


@pytest.mark.parametrize("option", ["unknown", "sasl.unknown"])
def test_kafka_scram_auth_unknown_options(option, caplog):
    with caplog.at_level(logging.WARNING):
        Kafka.ScramAuth.parse({"username": "user", "password": "some", "digest": "SHA-256", option: "value"})

    assert f"Options ['{option}'] are not known by KafkaScramAuth, are you sure they are valid?" in caplog.text


@pytest.mark.parametrize("digest", ["SHA-256", "SHA-512"])
def test_kafka_scram_auth_get_jaas_conf(spark_mock, digest):
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        auth=Kafka.ScramAuth(username="user", password="password", digest=digest),
    )

    conf = kafka.auth.get_options(kafka)
    assert conf == {
        "sasl.mechanism": f"SCRAM-{digest}",
        "sasl.jaas.config": (
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            'username="user" '
            'password="password";'
        ),
    }


@pytest.mark.parametrize("digest", ["SHA-256", "SHA-512"])
def test_kafka_scram_auth_get_jaas_conf_custom_properties(spark_mock, digest):
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        auth=Kafka.ScramAuth.parse(
            {
                "username": "user",
                "password": "password",
                "digest": digest,
                "sasl.login.class": "com.example.CustomScramLogin",
                "kafka.sasl.login.some.option": 1,
            },
        ),
    )

    conf = kafka.auth.get_options(kafka)
    assert conf == {
        "sasl.mechanism": f"SCRAM-{digest}",
        "sasl.jaas.config": (
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
            'username="user" '
            'password="password";'
        ),
        "sasl.login.class": "com.example.CustomScramLogin",
        "sasl.login.some.option": "1",
    }


def test_kafka_kerberos_auth_deploy_keytab_false(spark_mock, create_keytab, keytab_md5):
    keytab_path = Path("kafka_user_" + keytab_md5 + ".keytab")
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        auth=Kafka.KerberosAuth(
            principal="user",
            keytab=create_keytab,
            deploy_keytab=False,
        ),
    )

    with kafka:
        conf = kafka.auth.get_options(kafka)
        assert conf == {
            "sasl.mechanism": "GSSAPI",
            "sasl.jaas.config": (
                "com.sun.security.auth.module.Krb5LoginModule required "
                'principal="user" '
                f'keyTab="{create_keytab}" '
                'serviceName="kafka" '
                "renewTicket=true "
                "storeKey=true "
                "useKeyTab=true "
                "useTicketCache=false;"
            ),
            "sasl.kerberos.service.name": "kafka",
        }
        assert not keytab_path.exists()

    assert not keytab_path.exists()


def test_kafka_kerberos_auth_deploy_keytab_true(spark_mock, create_keytab, keytab_md5):
    keytab_path = Path("kafka_user_" + keytab_md5 + ".keytab")

    # deploy_keytab=True by default
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        auth=Kafka.KerberosAuth(
            principal="user",
            keytab=create_keytab,
        ),
    )

    with kafka:
        conf = kafka.auth.get_options(kafka)
        assert conf == {
            "sasl.mechanism": "GSSAPI",
            "sasl.jaas.config": (
                "com.sun.security.auth.module.Krb5LoginModule required "
                'principal="user" '
                f'keyTab="{keytab_path.name}" '
                'serviceName="kafka" '
                "renewTicket=true "
                "storeKey=true "
                "useKeyTab=true "
                "useTicketCache=false;"
            ),
            "sasl.kerberos.service.name": "kafka",
        }

        assert keytab_path.exists()
        assert keytab_path.read_text() == create_keytab.read_text()

    assert not keytab_path.exists()


def test_kafka_kerberos_auth_no_keytab(spark_mock):
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        auth=Kafka.KerberosAuth(
            principal="user",
            useKeyTab=False,
            useTicketCache=True,
        ),
    )

    with kafka:
        conf = kafka.auth.get_options(kafka)
        assert conf == {
            "sasl.mechanism": "GSSAPI",
            "sasl.jaas.config": (
                "com.sun.security.auth.module.Krb5LoginModule required "
                'principal="user" '
                'serviceName="kafka" '
                "renewTicket=true "
                "storeKey=true "
                "useKeyTab=false "
                "useTicketCache=true;"
            ),
            "sasl.kerberos.service.name": "kafka",
        }


def test_kafka_kerberos_auth_custom_jaas_conf_options(spark_mock, create_keytab):
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        auth=Kafka.KerberosAuth(
            principal="user",
            keytab=create_keytab,
            deploy_keytab=False,
            debug=True,
        ),
    )

    conf = kafka.auth.get_options(kafka)
    assert conf == {
        "sasl.mechanism": "GSSAPI",
        "sasl.jaas.config": (
            "com.sun.security.auth.module.Krb5LoginModule required "
            'principal="user" '
            f'keyTab="{create_keytab}" '
            'serviceName="kafka" '
            "renewTicket=true "
            "storeKey=true "
            "useKeyTab=true "
            "useTicketCache=false "
            "debug=true;"
        ),
        "sasl.kerberos.service.name": "kafka",
    }


def test_kafka_kerberos_auth_custom_kafka_conf_options(spark_mock, create_keytab):
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        auth=Kafka.KerberosAuth.parse(
            {
                "principal": "user",
                "keytab": create_keytab,
                "deploy_keytab": False,
                "sasl.kerberos.kinit.cmd": "/usr/bin/kinit",
                "kafka.sasl.kerberos.min.time.before.relogin": 60000,
            },
        ),
    )

    conf = kafka.auth.get_options(kafka)
    assert conf == {
        "sasl.mechanism": "GSSAPI",
        "sasl.jaas.config": (
            "com.sun.security.auth.module.Krb5LoginModule required "
            'principal="user" '
            f'keyTab="{create_keytab}" '
            'serviceName="kafka" '
            "renewTicket=true "
            "storeKey=true "
            "useKeyTab=true "
            "useTicketCache=false;"
        ),
        "sasl.kerberos.service.name": "kafka",
        "sasl.kerberos.kinit.cmd": "/usr/bin/kinit",
        "sasl.kerberos.min.time.before.relogin": "60000",
    }


def test_kafka_plaintext_protocol_with_auth(spark_mock):
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        auth=Kafka.BasicAuth(
            user="user",
            password="abc",
        ),
    )
    with kafka:
        assert kafka.protocol.get_options(kafka) == {"security.protocol": "SASL_PLAINTEXT"}


def test_kafka_plaintext_protocol_without_auth(spark_mock):
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
    )

    with kafka:
        assert kafka.protocol.get_options(kafka) == {"security.protocol": "PLAINTEXT"}


def test_kafka_normalize_cluster_name_hook(request, spark_mock):
    @Kafka.Slots.normalize_cluster_name.bind
    @hook
    def normalize_cluster_name(cluster: str):
        return cluster.lower().replace("_", "-")

    request.addfinalizer(normalize_cluster_name.disable)

    assert Kafka(cluster="kafka-cluster", spark=spark_mock, addresses=["192.168.1.1"]).cluster == "kafka-cluster"
    assert Kafka(cluster="kafka_cluster", spark=spark_mock, addresses=["192.168.1.1"]).cluster == "kafka-cluster"
    assert Kafka(cluster="KAFKA-CLUSTER", spark=spark_mock, addresses=["192.168.1.1"]).cluster == "kafka-cluster"


def test_kafka_get_known_clusters_hook(request, spark_mock):
    @Kafka.Slots.get_known_clusters.bind
    @hook
    def get_known_clusters():
        return {"cluster", "local"}

    request.addfinalizer(get_known_clusters.disable)

    assert Kafka(cluster="cluster", spark=spark_mock, addresses=["192.168.1.1"]).cluster in get_known_clusters()
    assert Kafka(cluster="local", spark=spark_mock, addresses=["192.168.1.1"]).cluster in get_known_clusters()
    error_msg = "Cluster 'unknown-cluster' is not in the known clusters list: ['cluster', 'local']"
    with pytest.raises(ValueError, match=re.escape(error_msg)):
        Kafka(cluster="unknown-cluster", spark=spark_mock, addresses=["192.168.1.1", "192.168.1.2"])


def test_kafka_normalize_address_hook(request, spark_mock):
    @Kafka.Slots.normalize_address.bind
    @hook
    def normalize_address(address: str, cluster: str):
        if cluster == "kafka-cluster":
            return f"{address}:9093"
        elif cluster == "local":
            return f"{address}:9092"
        return None

    request.addfinalizer(normalize_address.disable)

    assert Kafka(cluster="kafka-cluster", spark=spark_mock, addresses=["192.168.1.1"]).addresses == ["192.168.1.1:9093"]
    assert Kafka(cluster="local", spark=spark_mock, addresses=["localhost"]).addresses == ["localhost:9092"]


def test_kafka_get_cluster_addresses_hook(request, spark_mock):
    @Kafka.Slots.get_cluster_addresses.bind
    @hook
    def get_cluster_addresses(cluster: str):
        if cluster == "kafka-cluster":
            return ["192.168.1.1", "192.168.1.2"]
        return None

    request.addfinalizer(get_cluster_addresses.disable)

    assert Kafka(cluster="kafka-cluster", spark=spark_mock).addresses == [
        "192.168.1.1",
        "192.168.1.2",
    ]

    with pytest.raises(ValueError, match="Cluster 'kafka-cluster' does not contain addresses {'192.168.1.3'}"):
        Kafka(cluster="kafka-cluster", spark=spark_mock, addresses=["192.168.1.1", "192.168.1.3"])


@pytest.mark.parametrize(
    "options, value",
    [
        ({}, KafkaTopicExistBehaviorKafka.APPEND),
        ({"if_exists": "append"}, KafkaTopicExistBehaviorKafka.APPEND),
        ({"if_exists": "error"}, KafkaTopicExistBehaviorKafka.ERROR),
    ],
)
def test_kafka_write_options_if_exists(options, value):
    assert Kafka.WriteOptions(**options).if_exists == value


@pytest.mark.parametrize(
    "options, message",
    [
        (
            {"mode": "append"},
            "Parameter `mode` is not allowed. Please use `if_exists` parameter instead.",
        ),
        (
            {"mode": "error"},
            "Parameter `mode` is not allowed. Please use `if_exists` parameter instead.",
        ),
    ],
)
def test_kafka_write_options_mode_restricted(options, message):
    with pytest.raises(ValueError, match=re.escape(message)):
        Kafka.WriteOptions(**options)


@pytest.mark.parametrize(
    "options",
    [
        # disallowed mode
        {"if_exists": "ignore"},
        # wrong mode
        {"if_exists": "wrong_mode"},
    ],
)
def test_kafka_write_options_mode_wrong(options):
    with pytest.raises(ValueError, match="value is not a valid enumeration member"):
        Kafka.WriteOptions(**options)


@pytest.mark.parametrize(
    "prefix",
    ["", "kafka."],
)
def test_kafka_ssl_protocol_with_raw_strings(spark_mock, prefix):
    params = {
        f"{prefix}ssl.keystore.type": "PEM",
        f"{prefix}ssl.keystore.certificate.chain": "<certificate-chain-here>",
        f"{prefix}ssl.keystore.key": "<private-key_string>",
        f"{prefix}ssl.truststore.type": "PEM",
        f"{prefix}ssl.truststore.certificates": "<trusted-certificates>",
    }
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        protocol=Kafka.SSLProtocol.parse(params),
    )

    options = kafka.protocol.get_options(kafka)

    assert options == {
        "ssl.keystore.type": "PEM",
        "ssl.keystore.certificate.chain": "<certificate-chain-here>",
        "ssl.keystore.key": "<private-key_string>",
        "ssl.truststore.type": "PEM",
        "ssl.truststore.certificates": "<trusted-certificates>",
        "security.protocol": "SSL",
    }


@pytest.mark.parametrize(
    "keystore_type,truststore_type",
    [
        ("PEM", "PEM"),
        ("JKS", "JKS"),
    ],
)
def test_kafka_ssl_protocol_with_file_paths(
    spark_mock,
    create_temp_file,
    keystore_type,
    truststore_type,
):
    keystore_location = create_temp_file
    truststore_location = create_temp_file

    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        protocol=Kafka.SSLProtocol(
            keystore_type=keystore_type,
            keystore_location=keystore_location,
            key_password="<private_key_password>",
            truststore_type=truststore_type,
            truststore_location=truststore_location,
        ),
    )

    options = kafka.protocol.get_options(kafka)

    assert options == {
        "ssl.keystore.type": keystore_type,
        "ssl.keystore.location": os.fspath(keystore_location),
        "ssl.key.password": "<private_key_password>",
        "ssl.truststore.type": truststore_type,
        "ssl.truststore.location": os.fspath(truststore_location),
        "security.protocol": "SSL",
    }


def test_kafka_ssl_protocol_pem_certificates_as_file_paths_error(spark_mock):
    error_msg = "File '/not/existing/path' does not exist"
    with pytest.raises(FileNotFoundError, match=re.escape(error_msg)):
        Kafka(
            spark=spark_mock,
            addresses=["some_address"],
            cluster="cluster",
            protocol=Kafka.SSLProtocol(
                keystore_type="PEM",
                keystore_location="/not/existing/path",
                key_password="<private_key_password>",
                truststore_type="PEM",
                truststore_location="/not/existing/path",
            ),
        )


def test_kafka_ssl_protocol_with_extra_fields(spark_mock):
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        protocol=Kafka.SSLProtocol.parse(
            {
                "ssl.keystore.type": "PEM",
                "ssl.keystore.certificate.chain": "<certificate-chain-here>",
                "ssl.keystore.key": "<private-key_string>",
                "ssl.truststore.type": "PEM",
                "ssl.truststore.certificates": "<trusted-certificates>",
                "ssl.enabled.protocols": "TLSv1.2",
                "kafka.ssl.endpoint.some.value": True,
            },
        ),
    )

    assert kafka.protocol.get_options(kafka) == {
        "ssl.keystore.type": "PEM",
        "ssl.keystore.certificate.chain": "<certificate-chain-here>",
        "ssl.keystore.key": "<private-key_string>",
        "ssl.truststore.type": "PEM",
        "ssl.truststore.certificates": "<trusted-certificates>",
        "ssl.enabled.protocols": "TLSv1.2",
        "ssl.endpoint.some.value": "true",
        "security.protocol": "SSL",
    }


def test_kafka_ssl_protocol_with_basic_auth(spark_mock):
    kafka = Kafka(
        spark=spark_mock,
        addresses=["some_address"],
        cluster="cluster",
        protocol=Kafka.SSLProtocol.parse(
            {
                "ssl.keystore.type": "PEM",
                "ssl.keystore.certificate.chain": "<certificate-chain-here>",
                "ssl.keystore.key": "<private-key_string>",
                "ssl.truststore.type": "PEM",
                "ssl.truststore.certificates": "<trusted-certificates>",
            },
        ),
        auth=Kafka.BasicAuth(
            user="user",
            password="abc",
        ),
    )
    with kafka:
        assert kafka.protocol.get_options(kafka) == {
            "ssl.keystore.type": "PEM",
            "ssl.keystore.certificate.chain": "<certificate-chain-here>",
            "ssl.keystore.key": "<private-key_string>",
            "ssl.truststore.type": "PEM",
            "ssl.truststore.certificates": "<trusted-certificates>",
            "security.protocol": "SASL_SSL",
        }
