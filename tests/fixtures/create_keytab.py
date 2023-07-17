from pathlib import Path

import pytest


@pytest.fixture()
def create_keytab(tmp_path_factory):
    path = Path(tmp_path_factory.mktemp("data") / "keytab")
    path.write_text("content")

    return path
