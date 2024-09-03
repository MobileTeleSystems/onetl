import pytest

pytestmark = [pytest.mark.s3, pytest.mark.file_connection, pytest.mark.connection]


def test_s3_connection():
    from onetl.connection import S3

    conn = S3(
        host="some_host",
        access_key="access key",
        secret_key="some key",
        bucket="bucket",
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


def test_s3_connection_with_session_token():
    from onetl.connection import S3

    conn = S3(
        host="some_host",
        access_key="access_key",
        secret_key="some key",
        session_token="some token",
        bucket="bucket",
    )

    assert conn.session_token != "some token"
    assert conn.session_token.get_secret_value() == "some token"

    assert "some token" not in repr(conn)


def test_s3_connection_https():
    from onetl.connection import S3

    conn = S3(
        host="some_host",
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucket",
        protocol="https",
    )

    assert conn.protocol == "https"
    assert conn.port == 443
    assert conn.instance_url == "s3://some_host:443/bucket"
    assert str(conn) == "S3[some_host:443/bucket]"


def test_s3_connection_http():
    from onetl.connection import S3

    conn = S3(
        host="some_host",
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucket",
        protocol="http",
    )

    assert conn.protocol == "http"
    assert conn.port == 80
    assert conn.instance_url == "s3://some_host:80/bucket"
    assert str(conn) == "S3[some_host:80/bucket]"


@pytest.mark.parametrize("protocol", ["http", "https"])
def test_s3_connection_with_port(protocol):
    from onetl.connection import S3

    conn = S3(
        host="some_host",
        port=9000,
        access_key="access_key",
        secret_key="secret_key",
        bucket="bucket",
        protocol=protocol,
    )

    assert conn.protocol == protocol
    assert conn.port == 9000
    assert conn.instance_url == "s3://some_host:9000/bucket"
    assert str(conn) == "S3[some_host:9000/bucket]"
