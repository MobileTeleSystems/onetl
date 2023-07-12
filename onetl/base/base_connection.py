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

from abc import ABC, abstractmethod
from typing import TypeVar

T = TypeVar("T")


class BaseConnection(ABC):
    """
    Generic connection class
    """

    @abstractmethod
    def check(self: T) -> T:
        """Check source availability. |support_hooks|

        If not, an exception will be raised.

        Returns
        -------
        Connection itself

        Raises
        ------
        RuntimeError
            If the connection is not available

        Examples
        --------

        .. code:: python

            connection.check()
        """
