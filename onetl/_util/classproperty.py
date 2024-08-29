# SPDX-FileCopyrightText: 2021-2024 MTS PJSC
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations


class classproperty(property):  # noqa: N801
    """
    Like ``@property``, but can be used to decorate class methods.

    Examples
    --------

    >>> class My:
    ...    @classproperty
    ...    def attribute(cls):
    ...        return 123
    >>> # no call
    >>> My.attribute
    123
    """

    def __init__(self, f):
        self.f = f

    def __get__(self, obj, objtype=None):
        return self.f(objtype or type(obj))
