import re
import shutil
from pathlib import Path

import pytest

pytestmark = [pytest.mark.sftp, pytest.mark.file_connection, pytest.mark.connection]


def test_sftp_connection_anonymous():
    from onetl.connection import SFTP, FileConnection

    conn = SFTP(host="some_host")
    assert isinstance(conn, FileConnection)
    assert conn.host == "some_host"
    assert conn.port == 22
    assert not conn.user
    assert not conn.password
    assert not conn.key_file
    assert conn.instance_url == "sftp://some_host:22"
    assert str(conn) == "SFTP[some_host:22]"


def test_sftp_connection_with_port():
    from onetl.connection import SFTP

    conn = SFTP(host="some_host", port=500)

    assert conn.port == 500
    assert conn.instance_url == "sftp://some_host:500"
    assert str(conn) == "SFTP[some_host:500]"


def test_sftp_connection_with_password():
    from onetl.connection import SFTP

    conn = SFTP(host="some_host", user="some_user", password="pwd")
    assert conn.user == "some_user"
    assert conn.password != "pwd"
    assert conn.password.get_secret_value() == "pwd"
    assert not conn.key_file
    assert conn.instance_url == "sftp://some_host:22"
    assert str(conn) == "SFTP[some_host:22]"

    assert "pwd" not in repr(conn)


def test_sftp_connection_with_key_file(request, tmp_path_factory):
    from onetl.connection import SFTP

    folder: Path = tmp_path_factory.mktemp("key_file")
    key_file = folder / "id_rsa"
    key_file.touch()

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    conn = SFTP(host="some_host", user="some_user", key_file=key_file)
    assert conn.user == "some_user"
    assert not conn.password
    assert conn.key_file == key_file


def test_sftp_connection_key_file_does_not_exist():
    from onetl.connection import SFTP

    with pytest.raises(ValueError, match="Path does not point to a file"):
        SFTP(host="some_host", user="some_user", key_file="/path/to/key_file")


def test_sftp_connection_keytab_is_directory(request, tmp_path_factory):
    from onetl.connection import SFTP

    folder: Path = tmp_path_factory.mktemp("key_file")
    key_file = folder / "id_rsa"
    key_file.mkdir(exist_ok=True, parents=True)

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    with pytest.raises(ValueError, match="Path does not point to a file"):
        SFTP(host="some_host", user="some_user", key_file=key_file)


def test_sftp_connection_without_mandatory_args():
    from onetl.connection import SFTP

    with pytest.raises(ValueError):
        SFTP()


def test_sftp_connection_with_deprecated_params():
    from onetl.connection import SFTP

    msg = "Option `host_key_check` is deprecated since v0.16.0 and will be removed in v1.0.0. "
    with pytest.warns(UserWarning, match=re.escape(msg)):
        conn = SFTP(
            host="some_host",
            user="some_user",
            password="pwd",
            host_key_check=True,
            extra={"something": "abc"},
        )
    assert conn.extra.host_key_check is True
    assert conn.extra.something == "abc"

    msg = "Option `timeout` is deprecated since v0.16.0 and will be removed in v1.0.0. "
    with pytest.warns(UserWarning, match=re.escape(msg)):
        conn = SFTP(
            host="some_host",
            user="some_user",
            password="pwd",
            timeout=10,
            extra={"something": "abc"},
        )
    assert conn.extra.timeout == 10
    assert conn.extra.something == "abc"

    msg = "Option `compress` is deprecated since v0.16.0 and will be removed in v1.0.0. "
    with pytest.warns(UserWarning, match=re.escape(msg)):
        conn = SFTP(
            host="some_host",
            user="some_user",
            password="pwd",
            compress=True,
            extra={"something": "abc"},
        )
    assert conn.extra.compress is True
    assert conn.extra.something == "abc"


def test_sftp_connection_with_extra():
    from onetl.connection import SFTP

    conn = SFTP(
        host="some_host",
        user="some_user",
        password="pwd",
    )
    assert conn.extra.host_key_check is False
    assert conn.extra.timeout is None
    assert conn.extra.banner_timeout is None
    assert conn.extra.auth_timeout is None
    assert conn.extra.channel_timeout is None
    assert conn.extra.compress is False

    conn = SFTP(
        host="some_host",
        user="some_user",
        password="pwd",
        extra=SFTP.Extra(
            host_key_check=True,
            timeout=1,
            banner_timeout=1,
            auth_timeout=1,
            channel_timeout=1,
            compress=True,
            something="abc",
        ),
    )

    assert conn.extra.host_key_check is True
    assert conn.extra.timeout == 1
    assert conn.extra.banner_timeout == 1
    assert conn.extra.auth_timeout == 1
    assert conn.extra.channel_timeout == 1
    assert conn.extra.compress is True
    assert conn.extra.something == "abc"

    conn = SFTP(
        host="some_host",
        user="some_user",
        password="pwd",
        extra={
            "host_key_check": True,
            "timeout": 1,
            "banner_timeout": 1,
            "auth_timeout": 1,
            "channel_timeout": 1,
            "compress": True,
            "something": "abc",
        },
    )

    assert conn.extra.host_key_check is True
    assert conn.extra.timeout == 1
    assert conn.extra.banner_timeout == 1
    assert conn.extra.auth_timeout == 1
    assert conn.extra.channel_timeout == 1
    assert conn.extra.compress is True
    assert conn.extra.something == "abc"
