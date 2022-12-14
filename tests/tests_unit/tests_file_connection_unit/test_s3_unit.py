from onetl.connection import S3


def test_s3_connection():
    s3 = S3(
        host="some_host",
        port=9000,
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucket",
    )

    assert s3.host == "some_host"
    assert s3.access_key == "access_key"
    assert s3.secret_key != "pwd"
    assert s3.secret_key.get_secret_value() == "secret_key"
    assert s3.port == 9000
    assert s3.instance_url == "s3://some_host:9000"


def test_s3_connection_sec():
    s3 = S3(
        host="some_host",
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucket",
        protocol="https",
    )

    assert s3.host == "some_host"
    assert s3.access_key == "access_key"
    assert s3.secret_key != "pwd"
    assert s3.secret_key.get_secret_value() == "secret_key"
    assert s3.port == 443
    assert s3.instance_url == "s3://some_host:443"


def test_s3_connection_no_sec():
    s3 = S3(
        host="some_host",
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucket",
        protocol="http",
    )

    assert s3.host == "some_host"
    assert s3.access_key == "access_key"
    assert s3.secret_key != "pwd"
    assert s3.secret_key.get_secret_value() == "secret_key"
    assert s3.port == 80
    assert s3.instance_url == "s3://some_host:80"
