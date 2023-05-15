from onetl.hwm.store import register_hwm_store_class
from onetl.hwm.store.base_hwm_store import BaseHWMStore


@register_hwm_store_class("dummy")
class DummyHWMStore(BaseHWMStore):
    pass
