# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from etl_entities.hwm import FileHWM
from pydantic import field_validator

from onetl.base import BaseFileFilter, PathProtocol
from onetl.impl import BaseModel


class FileHWMFilter(BaseFileFilter, BaseModel):
    """Filter files which are not covered by FileHWM.

    !!! warning

        Only for onETL internal use.

    Parameters
    ----------

    hwm : etl_entities.hwm.FileHWM

        File HWM instance
    """

    hwm: FileHWM

    # etl-entities v1 uses pydantic v1 models
    # which are not compatible with pydantic v2.
    # using a plain validator here
    @field_validator("hwm", mode="plain")
    @classmethod
    def validate_hwm(cls, value):
        if not isinstance(value, FileHWM):
            return FileHWM.parse_obj(value)
        return value

    def match(self, path: PathProtocol) -> bool:
        if path.is_dir():
            return True

        return not self.hwm.covers(path)

    def __str__(self):
        return self.hwm.name

    def __repr__(self):
        return f"{self.hwm.__class__.__name__}(name={self.hwm.name!r})"
