import logging
import secrets

import pytest

from onetl.strategy.hwm_store import AtlasHWMStore, HWMStoreManager, YAMLHWMStore


def test_hwm_store_atlas(caplog):
    user = secrets.token_hex()
    password = secrets.token_hex()
    hwm_store = AtlasHWMStore(
        url="http://some.atlas.url",
        user=user,
        password=password,
    )

    assert hwm_store.url == "http://some.atlas.url"
    assert hwm_store.user == user
    assert hwm_store.password != password
    assert hwm_store.password.get_secret_value() == password

    with caplog.at_level(logging.INFO):
        with hwm_store as store:
            assert HWMStoreManager.get_current() == store

    assert "|onETL| Using AtlasHWMStore as HWM Store" in caplog.text
    assert "url = 'http://some.atlas.url'" in caplog.text
    assert f"user = '{user}'" in caplog.text
    assert password not in caplog.text

    assert HWMStoreManager.get_current() != hwm_store
    assert isinstance(HWMStoreManager.get_current(), YAMLHWMStore)


def test_hwm_store_atlas_no_credentials(caplog):
    hwm_store = AtlasHWMStore(
        url="http://some.atlas.url",
    )

    assert hwm_store.url == "http://some.atlas.url"
    assert not hwm_store.user
    assert not hwm_store.password

    with caplog.at_level(logging.INFO):
        with hwm_store as store:
            assert HWMStoreManager.get_current() == store

    assert "|onETL| Using AtlasHWMStore as HWM Store" in caplog.text
    assert "url = 'http://some.atlas.url'" in caplog.text
    assert "user =" not in caplog.text
    assert "password =" not in caplog.text

    assert HWMStoreManager.get_current() != hwm_store
    assert isinstance(HWMStoreManager.get_current(), YAMLHWMStore)


@pytest.mark.parametrize(
    "user, password",
    [
        (None, "pwd"),  # no user
        ("usr", None),  # no password
    ],
)
def test_hwm_store_atlas_wrong_credentials(user, password):
    error_msg = "You can pass to AtlasHWMStore only both `user` and `password`, or none of them"
    with pytest.raises(ValueError, match=error_msg):
        AtlasHWMStore(
            url="http://some.url",
            user=user,
            password=password,
        )
