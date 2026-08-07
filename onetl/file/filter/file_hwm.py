# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from etl_entities.hwm import FileHWM

# using pydantic v1 for backward compatibility with etl-entities 3.x
try:
    from pydantic.v1 import BaseModel
except (ImportError, AttributeError):
    from pydantic import BaseModel  # type: ignore[no-redef, assignment]

from onetl.base import BaseFileFilter, PathProtocol


class FileHWMFilter(BaseFileFilter, BaseModel):
    """Filter files which are not covered by FileHWM.

    !!! warning

        Only for onETL internal use.

    Parameters
    ----------

    hwm : etl_entities.hwm.FileHWM

        File HWM instance
    """

    class Config:
        frozen = True
        extra = "forbid"
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
