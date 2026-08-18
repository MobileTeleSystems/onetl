from unittest.mock import Mock

import pytest

from onetl.connection import SparkS3

pytestmark = [pytest.mark.s3, pytest.mark.file_df_connection, pytest.mark.connection]


@pytest.mark.parametrize(
    ("spark_version", "scala_version", "package"),
    [
        # Detect using pyspark version
        (None, None, "org.apache.spark:spark-hadoop-cloud_{scala_ver}:{pyspark_ver}"),
        # Override Spark version
        ("3.5.8", None, "org.apache.spark:spark-hadoop-cloud_2.12:3.5.8"),
        # Override Scala version
        (None, "2.13", "org.apache.spark:spark-hadoop-cloud_2.13:{pyspark_ver}"),
        ("3.5.8", "2.13", "org.apache.spark:spark-hadoop-cloud_2.13:3.5.8"),
        # Scala version contain three digits when only two needed
        ("3.5.8", "2.13.1", "org.apache.spark:spark-hadoop-cloud_2.13:3.5.8"),
    ],
)
def test_spark_s3_get_packages(spark_version, scala_version, package):
    import pyspark

    pyspark_ver = pyspark.__version__
    scala_ver = "2.12" if pyspark_ver.startswith("3") else "2.13"

    expected = package.format(pyspark_ver=pyspark_ver, scala_ver=scala_ver)
    assert SparkS3.get_packages(spark_version=spark_version, scala_version=scala_version) == [expected]


def test_spark_s3_missing_package(spark_no_packages):
    spark_no_packages._jvm = Mock()
    spark_no_packages._jvm.org.apache.hadoop.util.VersionInfo.getVersion = Mock(return_value="3.3.6")

    msg = "Cannot import Java class 'org.apache.hadoop.fs.s3a.S3AFileSystem'"
    with pytest.raises(ValueError, match=msg):
        SparkS3(
            host="some_host",
            access_key="access_key",
            secret_key="some key",
            session_token="some token",
            bucket="bucket",
            spark=spark_no_packages,
        )


def test_spark_s3_spark_stopped(spark_stopped):
    msg = "Spark session is stopped. Please recreate Spark session."
    with pytest.raises(ValueError, match=msg):
        SparkS3(
            host="some_host",
            access_key="access_key",
            secret_key="some key",
            session_token="some token",
            bucket="bucket",
            spark=spark_stopped,
        )


@pytest.fixture()
def spark_mock_hadoop_3(spark_mock):
    spark_mock._jvm = Mock()
    spark_mock._jvm.org.apache.hadoop.util.VersionInfo.getVersion = Mock(return_value="3.3.6")
    return spark_mock


def test_spark_s3(spark_mock_hadoop_3):
    conn = SparkS3(
        host="some_host",
        access_key="access key",
        secret_key="some key",
        bucket="bucket",
        spark=spark_mock_hadoop_3,
    )

    assert conn.host == "some_host"
    assert conn.access_key == "access key"
    assert conn.secret_key != "some key"
    assert conn.secret_key.get_secret_value() == "some key"
    assert conn.protocol == "https"
    assert conn.port == 443
    assert conn.instance_url == "s3://some_host:443/bucket"
    assert str(conn) == "S3[some_host:443/bucket]"

    assert "some key" not in repr(conn)


def test_spark_s3_with_protocol_https(spark_mock_hadoop_3):
    conn = SparkS3(
        host="some_host",
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucket",
        protocol="https",
        spark=spark_mock_hadoop_3,
    )

    assert conn.protocol == "https"
    assert conn.port == 443
    assert conn.instance_url == "s3://some_host:443/bucket"
    assert str(conn) == "S3[some_host:443/bucket]"


def test_spark_s3_with_protocol_http(spark_mock_hadoop_3):
    conn = SparkS3(
        host="some_host",
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucket",
        protocol="http",
        spark=spark_mock_hadoop_3,
    )

    assert conn.protocol == "http"
    assert conn.port == 80
    assert conn.instance_url == "s3://some_host:80/bucket"
    assert str(conn) == "S3[some_host:80/bucket]"


@pytest.mark.parametrize("protocol", ["http", "https"])
def test_spark_s3_with_port(spark_mock_hadoop_3, protocol):
    conn = SparkS3(
        host="some_host",
        port=9000,
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucket",
        protocol=protocol,
        spark=spark_mock_hadoop_3,
    )

    assert conn.protocol == protocol
    assert conn.port == 9000
    assert conn.instance_url == "s3://some_host:9000/bucket"
    assert str(conn) == "S3[some_host:9000/bucket]"


def test_spark_s3_with_path_style_access(spark_mock_hadoop_3):
    conn = SparkS3(
        host="some_host",
        bucket="bucket",
        path_style_access=True,
        spark=spark_mock_hadoop_3,
    )

    assert conn.path_style_access is True


def test_spark_s3_with_path_style_access_from_extra(spark_mock_hadoop_3):
    conn = SparkS3(
        host="some_host",
        bucket="bucket",
        spark=spark_mock_hadoop_3,
        extra={
            "path.style.access": True,
        },
    )

    assert conn.path_style_access is True


def test_spark_s3_without_path_style_access(spark_mock_hadoop_3):
    conn = SparkS3(
        host="some_host",
        bucket="bucket",
        spark=spark_mock_hadoop_3,
    )

    assert conn.path_style_access is False


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("attempts.maximum", 1),
        ("connection.establish.timeout", 300000),
        ("connection.timeout", 300000),
        ("committer.name", "magic"),
        ("connection.ssl.enabled", False),
        ("path.style.access", True),
        ("path.style.access", False),
        ("aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"),
    ],
)
@pytest.mark.parametrize("prefix", ["", "spark.hadoop.", "fs.s3a.", "fs.s3a.bucket.mybucket."])
def test_spark_s3_extra_allowed_options(name, value, prefix):
    extra = SparkS3.Extra.parse({prefix + name: value}).model_dump()
    assert extra[name] == value


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
        ("endpoint", "http://localhost:9010"),
        ("access.key", "onetl"),
        ("secret.key", "key"),
        ("session.token", "token"),
        ("com.amazonaws.services.s3a.enableV4", True),
        ("fs.s3.awsAccessKeyId", "onetl"),
        ("fs.s3n.awsAccessKeyId", "onetl"),
    ],
)
@pytest.mark.parametrize("prefix", ["", "spark.hadoop.", "fs.s3a.", "fs.s3a.bucket.mybucket."])
def test_spark_s3_extra_prohibited_options(name, value, prefix):
    msg = rf"Options \['{name}'\] are not allowed to use in a SparkS3Extra"
    with pytest.raises(ValueError, match=msg):
        SparkS3.Extra.parse({prefix + name: value})
