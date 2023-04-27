import pytest

pytestmark = [pytest.mark.s3, pytest.mark.file_connection, pytest.mark.connection]


def test_s3_connection():
    from onetl.connection import S3

    s3 = S3(
        host="some_host",
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucket",
    )

    assert s3.host == "some_host"
    assert s3.access_key == "access_key"
    assert s3.secret_key != "pwd"
    assert s3.secret_key.get_secret_value() == "secret_key"
    assert s3.protocol == "https"
    assert s3.port == 443
    assert s3.instance_url == "s3://some_host:443"

    assert "secret_key='secret_key'" not in str(s3)
    assert "secret_key='secret_key'" not in repr(s3)


def test_s3_connection_https():
    from onetl.connection import S3

    s3 = S3(
        host="some_host",
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucket",
        protocol="https",
    )

    assert s3.protocol == "https"
    assert s3.port == 443
    assert s3.instance_url == "s3://some_host:443"


def test_s3_connection_http():
    from onetl.connection import S3

    s3 = S3(
        host="some_host",
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucket",
        protocol="http",
    )

    assert s3.protocol == "http"
    assert s3.port == 80
    assert s3.instance_url == "s3://some_host:80"


@pytest.mark.parametrize("protocol", ["http", "https"])
def test_s3_connection_with_port(protocol):
    from onetl.connection import S3

    s3 = S3(
        host="some_host",
        port=9000,
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucket",
        protocol=protocol,
    )

    assert s3.protocol == protocol
    assert s3.port == 9000
    assert s3.instance_url == "s3://some_host:9000"
