from etl_entities.hwm_store import BaseHWMStore, register_hwm_store_class


@register_hwm_store_class("dummy")
class DummyHWMStore(BaseHWMStore):
    pass
