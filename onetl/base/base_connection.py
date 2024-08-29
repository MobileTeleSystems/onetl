# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
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
