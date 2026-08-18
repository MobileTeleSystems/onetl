# SPDX-FileCopyrightText: 2022-present MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from typing import Protocol, runtime_checkable


@runtime_checkable
class ContainsException(Protocol):
    """
    Protocol for objects containing `.exception` attribute
    """

    @property
    def exception(self) -> Exception:
        """
        Exception object with traceback
        """
