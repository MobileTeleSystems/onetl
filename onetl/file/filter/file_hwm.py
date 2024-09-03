# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
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
