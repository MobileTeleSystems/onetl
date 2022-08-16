from __future__ import annotations

from logging import getLogger

from pydantic import BaseModel, PrivateAttr

from onetl.base.base_file_limit import BaseFileLimit
from onetl.log import log_with_indent

log = getLogger(__name__)


class FileLimit(BaseFileLimit, BaseModel):
    """Limits the number of uploaded files

    Parameters
    ----------

    count_limit : int, default = 100

        Number of downloaded files at a time.

    Examples
    --------

    Сreate a FileLimit object and set the amount in it:

    .. code:: python

        limit = FileLimit(count_limit=1500)

    If you create a :obj:`onetl.core.file_downloader.file_downloader.FileDownloader` object without
    specifying the limit option, it will download with a limit of 100 files.
    """

    count_limit: int = 100

    _counter: int = PrivateAttr(default_factory=int)
    _is_reached: bool = PrivateAttr(default_factory=bool)

    def reset_state(self):
        self._counter = 0  # noqa: WPS601

    def verify(self) -> bool:
        if self.is_reached:
            return True

        self._increase_counter()
        return self.is_reached

    @property
    def is_reached(self) -> bool:
        return self._counter >= self.count_limit

    def log_options(self, indent: int = 0):
        for key, value in self.__dict__.items():  # noqa: WPS528
            log_with_indent(f"{key} = {value!r}", indent=indent)

    def _increase_counter(self):
        self._counter += 1  # noqa: WPS601
