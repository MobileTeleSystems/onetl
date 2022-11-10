from onetl.hwm.store import HWMStoreClassRegistry


def test_autoreg():
    # check that the class is registered even without an explicit import
    value = HWMStoreClassRegistry.get("dummy")
    assert value

    # check that the class is really what we expect
    from dummy import DummyHWMStore

    assert value == DummyHWMStore
