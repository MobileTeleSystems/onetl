import pytest


def test_file_connection_without_host_and_user(file_connection_class):
    with pytest.raises(TypeError):
        file_connection_class()  # noqa: F841
