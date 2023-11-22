from etl_entities.old_hwm import ColumnHWM as OldColumnHWM

from onetl.hwm.auto_hwm import AutoHWM


# convert old hwm to new ones (from etl_entities<2.0 to etl_entities>=2.0)
def old_hwm_to_new_hwm(old_hwm: OldColumnHWM) -> AutoHWM:
    hwm = AutoHWM(
        name=old_hwm.qualified_name,
        entity=old_hwm.column.name,
        value=old_hwm.value,
        modified_time=old_hwm.modified_time,
    )
    return hwm  # noqa: WPS331


# ColumnHWM has abstract method serialize_value, so it's not possible to create a class instance
# small hack to bypass this exception
class MockColumnHWM(OldColumnHWM):
    def serialize_value(self):
        """Fake implementation of ColumnHWM.serialize_value"""
