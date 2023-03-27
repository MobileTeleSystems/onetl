import shutil
from pathlib import Path

import pytest

from onetl.connection import SFTP

pytestmark = pytest.mark.sftp


def test_sftp_connection():
    sftp = SFTP(host="some_host")
    assert sftp.host == "some_host"
    assert sftp.port == 22
    assert not sftp.user
    assert not sftp.password
    assert not sftp.key_file


def test_sftp_connection_with_port():
    sftp = SFTP(host="some_host", port=500)
    assert sftp.host == "some_host"
    assert sftp.port == 500
    assert not sftp.user
    assert not sftp.password
    assert not sftp.key_file


def test_sftp_connection_with_password():
    sftp = SFTP(host="some_host", user="some_user", password="pwd")
    assert sftp.host == "some_host"
    assert sftp.port == 22
    assert sftp.user == "some_user"
    assert sftp.password != "pwd"
    assert sftp.password.get_secret_value() == "pwd"
    assert not sftp.key_file


def test_sftp_connection_with_key_file(request, tmp_path_factory):
    folder: Path = tmp_path_factory.mktemp("key_file")
    folder.mkdir(exist_ok=True, parents=True)
    key_file = folder / "id_rsa"
    key_file.touch()

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    sftp = SFTP(host="some_host", user="some_user", key_file=key_file)
    assert sftp.host == "some_host"
    assert sftp.port == 22
    assert sftp.user == "some_user"
    assert not sftp.password
    assert sftp.key_file == key_file


def test_sftp_connection_key_file_does_not_exist():
    with pytest.raises(ValueError, match='file or directory at path "/path/to/key_file" does not exist'):
        SFTP(host="some_host", user="some_user", key_file="/path/to/key_file")


def test_sftp_connection_keytab_is_directory(request, tmp_path_factory):
    folder: Path = tmp_path_factory.mktemp("key_file")
    key_file = folder / "id_rsa"
    key_file.mkdir(exist_ok=True, parents=True)

    def finalizer():
        shutil.rmtree(folder)

    request.addfinalizer(finalizer)

    with pytest.raises(ValueError, match=f'path "{key_file}" does not point to a file'):
        SFTP(host="some_host", user="some_user", key_file=key_file)


def test_sftp_connection_without_mandatory_args():
    with pytest.raises(ValueError):
        SFTP()
