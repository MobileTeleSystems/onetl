import sys

import onetl
from onetl.hwm.store import HWMStoreClassRegistry


def test_autoimport_success(request):
    def finalizer():
        sys.modules.pop("dummy", None)

    request.addfinalizer(finalizer)

    onetl.plugins_auto_import()
    assert "dummy" in sys.modules

    # check that the class is registered even without an explicit import
    value = HWMStoreClassRegistry.get("dummy")
    assert value

    # check that the module and class are really what we expect
    import dummy

    assert sys.modules["dummy"] is dummy
    assert value is dummy.DummyHWMStore


def test_autoimport_success_disabled(monkeypatch):
    sys.modules.pop("dummy", None)

    monkeypatch.setenv("ONETL_PLUGINS_ENABLED", "false")

    # plugin is not being imported
    onetl.plugins_auto_import()
    assert "dummy" not in sys.modules
