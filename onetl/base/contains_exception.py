# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing_extensions import Protocol, runtime_checkable


@runtime_checkable
class ContainsException(Protocol):
    """
    Protocol for objects containing ``.exception`` attribute
    """

    @property
    def exception(self) -> Exception:
        """
        Exception object with traceback
        """
