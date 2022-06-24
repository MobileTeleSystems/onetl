import pytest

from onetl.strategy import AtlasHWMStore


@pytest.mark.parametrize(
    "user, password",
    [
        (None, "pwd"),  # no user
        ("usr", None),  # no password
    ],
)
def test_hwm_store_integration_atlas_wrong_input(user, password):
    with pytest.raises(ValueError):
        AtlasHWMStore(
            url="http://some.url",
            user=user,
            password=password,
        )
