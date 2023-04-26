import logging

import pytest

from onetl.hwm.store import HWMStoreManager, MemoryHWMStore, YAMLHWMStore


def test_hwm_store_memory_context_manager(caplog):
    hwm_store = MemoryHWMStore()

    with caplog.at_level(logging.INFO):
        with hwm_store as store:
            assert HWMStoreManager.get_current() == store

    assert "|onETL| Using MemoryHWMStore as HWM Store" in caplog.text

    assert HWMStoreManager.get_current() != hwm_store
    assert isinstance(HWMStoreManager.get_current(), YAMLHWMStore)
