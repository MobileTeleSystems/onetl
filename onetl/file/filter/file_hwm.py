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

from __future__ import annotations

from etl_entities.hwm import FileHWM

from onetl.base import BaseFileFilter, PathProtocol
from onetl.impl import FrozenModel


class FileHWMFilter(BaseFileFilter, FrozenModel):
    """Filter files which are not covered by FileHWM.

    .. warning::

        Only for onETL internal use.

    Parameters
    ----------

    hwm : :obj:`etl_entities.hwm.FileHWM`

        File HWM instance
    """

    class Config:
        arbitrary_types_allowed = True

    hwm: FileHWM

    def match(self, path: PathProtocol) -> bool:
        if path.is_dir():
            return True

        return not self.hwm.covers(path)

    def __str__(self):
        return self.hwm.name

    def __repr__(self):
        return f"{self.hwm.__class__.__name__}(name={self.hwm.name!r})"
