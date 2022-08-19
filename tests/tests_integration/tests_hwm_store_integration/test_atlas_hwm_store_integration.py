import os
import secrets

import pytest

from onetl.strategy import AtlasHWMStore

ATLAS_HOST = os.environ.get("ONETL_ATLAS_CONN_HOST")
ATLAS_PORT = os.environ.get("ONETL_ATLAS_CONN_PORT")
ATLAS_URL = f"http://{ATLAS_HOST}:{ATLAS_PORT}"
ATLAS_USER = os.environ.get("ONETL_ATLAS_CONN_USER")
ATLAS_PASSWORD = os.environ.get("ONETL_ATLAS_CONN_PASSWORD")


@pytest.mark.parametrize(
    "url, user, password",
    [
        ("http://unknown.url", ATLAS_USER, ATLAS_PASSWORD),
        (ATLAS_HOST, ATLAS_USER, ATLAS_PASSWORD),  # closed port
        (f"{ATLAS_HOST}:{ATLAS_PORT}", ATLAS_USER, ATLAS_PASSWORD),  # no schema
        (ATLAS_URL, secrets.token_hex(5), ATLAS_PASSWORD),  # wrong user
        (ATLAS_URL, ATLAS_USER, secrets.token_hex(5)),  # wrong password
    ],
)
def test_hwm_store_integration_atlas_no_access(url, user, password, hwm_delta):
    hwm, _delta = hwm_delta
    store = AtlasHWMStore(
        url=url,
        user=user,
        password=password,
    )

    with pytest.raises(Exception):
        store.get(hwm.qualified_name)

    with pytest.raises(Exception):
        store.save(hwm)
