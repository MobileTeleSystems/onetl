import hashlib
from pathlib import Path

import pytest


@pytest.fixture
def create_keytab(tmp_path_factory):
    path = Path(tmp_path_factory.mktemp("data") / "some.keytab")
    path.write_text("content")

    return path


@pytest.fixture
def keytab_md5():
    return hashlib.md5(b"content").hexdigest()  # noqa: S324
