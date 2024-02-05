# SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from abc import ABC, abstractmethod

from onetl.base.path_protocol import PathProtocol


class BaseFileFilter(ABC):
    """
    Base file filter interface.

    Filters used by several onETL components, including :ref:`file-downloader` and :ref:`file-mover`,
    to determine if a file should be handled or not.

    All filters are stateless.
    """

    @abstractmethod
    def match(self, path: PathProtocol) -> bool:
        """
        Returns ``True`` if path is matching the filter, ``False`` otherwise

        Examples
        --------

        .. code:: python

            from onetl.impl import LocalPath

            assert filter.match(LocalPath("/path/to/file.csv"))
            assert not filter.match(LocalPath("/path/to/excluded.csv"))
            assert filter.match(LocalPath("/path/to/file.csv"))
        """
