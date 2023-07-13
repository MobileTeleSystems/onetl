import secrets
import shutil
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def resource_path_original():
    path = Path(__file__).parent.parent.parent / "resources"
    assert path.exists()
    return path


@pytest.fixture()
def resource_path(resource_path_original, tmp_path_factory):
    temp_dir = tmp_path_factory.mktemp("test_files") / secrets.token_hex(5)
    shutil.copytree(resource_path_original, temp_dir)
    return temp_dir


@pytest.fixture()
def test_files(resource_path):
    return [
        resource_path / "raw/ascii.txt",
        resource_path / "raw/utf-8.txt",
    ]
