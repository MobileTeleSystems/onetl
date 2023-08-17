from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def resource_path():
    path = Path(__file__).parent.parent.parent / "resources"
    assert path.exists()
    return path
