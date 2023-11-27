#  Copyright 2023 MTS (Mobile Telesystems)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import logging

from etl_entities.hwm import FileListHWM
from etl_entities.old_hwm import ColumnHWM as OldColumnHWM
from etl_entities.old_hwm import FileListHWM as OldFileListHWM

from onetl.hwm.auto_hwm import AutoHWM

log = logging.getLogger(__name__)


# convert old hwm to new ones (from etl_entities<2.0 to etl_entities>=2.0)
def old_hwm_to_new_hwm(old_hwm: OldColumnHWM) -> AutoHWM:
    log.warning(
        'Passing "hwm_column" in DBReader class is deprecated since version 0.10.0. It will be removed'
        " in future versions. Use hwm=DBReader.AutoHWM(...) class instead.",
    )
    hwm = AutoHWM(
        name=old_hwm.qualified_name,
        column=old_hwm.column.name,
        value=old_hwm.value,
        modified_time=old_hwm.modified_time,
    )
    return hwm  # noqa: WPS331


def old_file_hwm_to_new_file_hwm(old_file_hwm: OldFileListHWM) -> FileListHWM:
    log.warning(
        'Passing "hwm_type" in FileDownloader class is deprecated since version 0.10.0. It will be removed'
        ' in future versions. Use hwm=FileListHWM(name="...") class instead.',
    )
    file_hwm = FileListHWM(
        name=old_file_hwm.qualified_name,
        directory=old_file_hwm.source.full_name,
        value=old_file_hwm.value,
        modified_time=old_file_hwm.modified_time,
    )

    return file_hwm  # noqa: WPS331


# ColumnHWM has abstract method serialize_value, so it's not possible to create a class instance
# small hack to bypass this exception
class MockColumnHWM(OldColumnHWM):
    def serialize_value(self):
        """Fake implementation of ColumnHWM.serialize_value"""
