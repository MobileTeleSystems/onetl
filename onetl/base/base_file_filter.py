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
