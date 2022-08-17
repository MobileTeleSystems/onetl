from __future__ import annotations

from etl_entities import FileHWM
from pydantic import BaseModel

from onetl.base import BaseFileFilter, PathProtocol
from onetl.log import log_with_indent


class FileHWMFilter(BaseFileFilter, BaseModel):
    """Filter files which are not covered by FileHWM

    Parameters
    ----------

    hwm : FileHWM

        :obj:`etl_entities.FileHWM` instance
    """

    class Config:
        arbitrary_types_allowed = True

    hwm: FileHWM

    def match(self, path: PathProtocol) -> bool:
        if path.is_dir():
            return True

        return not self.hwm.covers(path)

    def log_options(self, indent: int = 0):
        log_with_indent(f"hwm_type = {self.hwm.__class__.__name__}", indent=indent)
        log_with_indent(f"qualified_name = {self.hwm.qualified_name!r}", indent=indent)

    def __str__(self):
        return self.hwm.qualified_name

    def __repr__(self):
        return f"{self.hwm.__class__.__name__}(qualified_name={self.hwm.qualified_name!r})"
