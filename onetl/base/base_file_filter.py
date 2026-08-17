# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from abc import ABC, abstractmethod

from onetl.base.path_protocol import PathProtocol


class BaseFileFilter(ABC):
    """
    Base file filter interface.

    Filters used by several onETL components, including [onetl.file.file_downloader.file_downloader.FileDownloader][] and [onetl.file.file_mover.file_mover.FileMover][],
    to determine if a file should be handled or not.

    All filters are stateless.

    !!! success "Added in 0.8.0"
    """

    @abstractmethod
    def match(self, path: PathProtocol) -> bool:
        """
        Returns `True` if path is matching the filter, `False` otherwise

        !!! success "Added in 0.8.0"

        Examples
        --------

        ```python
        >>> from onetl.impl import LocalPath
        >>> filter.match(LocalPath("/path/to/file.csv"))
        True
        >>> filter.match(LocalPath("/path/to/excluded.csv"))
        False
        >>> filter.match(LocalPath("/path/to/file.csv"))
        True
        ```
        """
