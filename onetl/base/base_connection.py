# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
import sys
from abc import ABC, abstractmethod
from typing import TypeVar

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

T = TypeVar("T")


class BaseConnection(ABC):
    """
    Generic connection class
    """

    @abstractmethod
    def check(self) -> Self:
        """Check source availability. [![support hooks](https://img.shields.io/badge/%20-support%20hooks-blue)][DBR-onetl-hooks]

        If not, an exception will be raised.

        Returns
        -------
        :
            Connection itself

        Raises
        ------
        RuntimeError
            If the connection is not available

        Examples
        --------

        ```python
        connection.check()
        ```
        """
